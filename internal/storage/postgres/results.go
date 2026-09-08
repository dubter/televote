package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/OWNER/televote/internal/domain"
)

// ResultRepo — агрегат результатов: монотонный процесс подсчёта и публикуемый
// результат после исключений.
//
// Отдельные голоса тут не хранятся и храниться не могут: в схеме нет связи
// «голос ↔ человек» (CLAUDE.md, «Приватность»).
type ResultRepo struct {
	db *pgxpool.Pool
}

// NewResultRepo создаёт репозиторий результатов.
func NewResultRepo(db *pgxpool.Pool) (*ResultRepo, error) {
	if db == nil {
		return nil, errors.New("postgres: ResultRepo без пула соединений")
	}
	return &ResultRepo{db: db}, nil
}

// Upsert записывает снимок счётчиков через GREATEST.
//
// Снапшотер читает из Redis АБСОЛЮТНЫЕ значения и пишет абсолютные — не дельты.
// Отсюда три свойства, на которых держится подсчёт:
//
//   - повтор тика безвреден: GREATEST(x, x) = x, поэтому ретрай после таймаута
//     не завышает результат, а дельта завысила бы;
//   - пропущенный цикл догоняется следующим, порядок тиков не важен;
//   - два снапшотера одновременно безопасны, и лидер-элекшн становится
//     оптимизацией, а не требованием корректности.
//
// GREATEST здесь — единственная защита от отката цифры назад. Redis теряет часть
// данных при failover и поднимается с меньшими счётчиками; присваивание вместо
// GREATEST уменьшило бы цифру в эфире на глазах у зрителей. Заменять нельзя
// (CLAUDE.md, «Чего не делать»).
//
// Опции, отсутствующие в a, не обнуляются: их строки просто не участвуют в
// запросе. Пропавшая из чтения Redis опция обязана сохранить прежнее значение.
func (r *ResultRepo) Upsert(ctx context.Context, pollID uuid.UUID, a domain.Aggregate) error {
	if a.Ballots < 0 {
		return fmt.Errorf("postgres: опрос %s: отрицательное число бюллетеней %d", pollID, a.Ballots)
	}

	// Порядок индексов детерминирован: два снапшотера, берущие блокировки строк
	// в разном порядке, встают в дедлок, и Postgres убивает одну транзакцию.
	indexes := sortedIndexes(a.Votes)
	idx := make([]int16, 0, len(indexes))
	votes := make([]int64, 0, len(indexes))
	for _, i := range indexes {
		v := a.Votes[i]
		if v < 0 {
			// GREATEST(текущее, -5) вернул бы текущее, и запись прошла бы без
			// ошибки, спрятав битый счётчик. Отрицательный счётчик — дефект
			// выше по стеку, и он должен быть виден.
			return fmt.Errorf("postgres: опрос %s, опция %d: отрицательный счётчик %d", pollID, i, v)
		}
		idx = append(idx, int16(i))
		votes = append(votes, v)
	}

	err := pgx.BeginFunc(ctx, r.db, func(tx pgx.Tx) error {
		const upsertResults = `
			INSERT INTO poll_results (poll_id, option_idx, votes, updated_at)
			SELECT $1, t.idx, t.votes, now()
			FROM unnest($2::smallint[], $3::bigint[]) AS t(idx, votes)
			ORDER BY t.idx
			ON CONFLICT (poll_id, option_idx) DO UPDATE
			SET votes = GREATEST(poll_results.votes, EXCLUDED.votes),
			    updated_at = now()`
		if _, err := tx.Exec(ctx, upsertResults, pollID, idx, votes); err != nil {
			return err
		}

		// Бюллетени монотонны по той же причине и тем же способом: проценты
		// считаются от них, и уехавший вниз знаменатель задрал бы все доли.
		const upsertStats = `
			INSERT INTO poll_stats (poll_id, ballots_total, updated_at)
			VALUES ($1, $2, now())
			ON CONFLICT (poll_id) DO UPDATE
			SET ballots_total = GREATEST(poll_stats.ballots_total, EXCLUDED.ballots_total),
			    updated_at = now()`
		_, err := tx.Exec(ctx, upsertStats, pollID, a.Ballots)
		return err
	})
	if err != nil {
		if isForeignKeyViolation(err) {
			return fmt.Errorf("postgres: опрос %s: %w", pollID, ErrNotFound)
		}
		return fmt.Errorf("postgres: запись снимка результатов опроса %s: %w", pollID, err)
	}
	return nil
}

// Get читает текущий агрегат процесса подсчёта.
//
// Опрос без голосов даёт пустой агрегат и nil: отсутствие снимка — это не
// ошибка, а нормальное состояние до первого тика снапшотера, и заставлять
// каждый вызывающий отличать ErrNotFound от «нулей» значит плодить ветку,
// которая всё равно сведётся к пустому агрегату.
func (r *ResultRepo) Get(ctx context.Context, pollID uuid.UUID) (domain.Aggregate, error) {
	a, err := r.aggregate(ctx, pollID, "poll_results", "poll_stats")
	if err != nil {
		return domain.Aggregate{}, fmt.Errorf("postgres: чтение результатов опроса %s: %w", pollID, err)
	}
	return a, nil
}

// SaveAdjusted записывает публикуемый результат с применёнными исключениями.
//
// Присваивание, а не GREATEST, и это осознанно противоположно Upsert:
// исключение накрученных подсетей УМЕНЬШАЕТ цифру, и монотонность здесь съела
// бы весь смысл операции. Ровно поэтому таблицы две — совместить монотонность
// подсчёта и исключение голосов в одной невозможно.
//
// poll_results не изменяется ни на строку: она остаётся аудитом того, что
// насчитал консьюмер, и должна расходиться с публикуемым результатом ровно на
// исключённое.
//
// excludedNets — /16-подсети, то есть агрегаты. Полных адресов здесь нет.
func (r *ResultRepo) SaveAdjusted(
	ctx context.Context, pollID uuid.UUID, a domain.Aggregate, excludedNets []string,
) error {
	if a.Ballots < 0 {
		return fmt.Errorf("postgres: опрос %s: отрицательное число бюллетеней %d", pollID, a.Ballots)
	}

	indexes := sortedIndexes(a.Votes)
	idx := make([]int16, 0, len(indexes))
	votes := make([]int64, 0, len(indexes))
	for _, i := range indexes {
		v := a.Votes[i]
		if v < 0 {
			return fmt.Errorf("postgres: опрос %s, опция %d: отрицательный счётчик %d", pollID, i, v)
		}
		idx = append(idx, int16(i))
		votes = append(votes, v)
	}

	// nil и пустой срез дают одно и то же: пустой JSON-массив. Различать их
	// незачем — «исключений не было» ровно одно состояние.
	if excludedNets == nil {
		excludedNets = []string{}
	}
	nets, err := json.Marshal(excludedNets)
	if err != nil {
		return fmt.Errorf("postgres: опрос %s: сериализация исключённых подсетей: %w", pollID, err)
	}

	err = pgx.BeginFunc(ctx, r.db, func(tx pgx.Tx) error {
		// DELETE перед вставкой, а не только upsert: набор опций мог сократиться,
		// и оставшаяся от прежней финализации строка опубликовала бы голоса
		// опции, которой в новом результате нет.
		const clear = `DELETE FROM poll_results_adjusted WHERE poll_id = $1`
		if _, err := tx.Exec(ctx, clear, pollID); err != nil {
			return err
		}

		const insertResults = `
			INSERT INTO poll_results_adjusted (poll_id, option_idx, votes)
			SELECT $1, t.idx, t.votes
			FROM unnest($2::smallint[], $3::bigint[]) AS t(idx, votes)
			ORDER BY t.idx`
		if _, err := tx.Exec(ctx, insertResults, pollID, idx, votes); err != nil {
			return err
		}

		// Наличие этой строки — признак «результат финализирован». Он отличает
		// «ещё не публиковали» от «опубликовали, и там нули».
		const upsertStats = `
			INSERT INTO poll_stats_adjusted (poll_id, ballots_total, excluded_nets, at)
			VALUES ($1, $2, $3::jsonb, now())
			ON CONFLICT (poll_id) DO UPDATE
			SET ballots_total = EXCLUDED.ballots_total,
			    excluded_nets = EXCLUDED.excluded_nets,
			    at            = now()`
		_, err := tx.Exec(ctx, upsertStats, pollID, a.Ballots, string(nets))
		return err
	})
	if err != nil {
		if isForeignKeyViolation(err) {
			return fmt.Errorf("postgres: опрос %s: %w", pollID, ErrNotFound)
		}
		return fmt.Errorf("postgres: запись публикуемого результата опроса %s: %w", pollID, err)
	}
	return nil
}

// GetAdjusted читает публикуемый результат и список исключённых подсетей.
//
// Возвращает ErrNotFound, пока опрос не финализирован — в отличие от Get.
// Асимметрия намеренная: «подсчёт ещё идёт» — рабочее состояние с осмысленными
// нулями, а «публикуемого результата ещё нет» — состояние, в котором отдавать
// нули в эфир нельзя, и вызывающий обязан это различать.
func (r *ResultRepo) GetAdjusted(
	ctx context.Context, pollID uuid.UUID,
) (domain.Aggregate, []string, error) {
	var (
		agg  domain.Aggregate
		nets []string
	)

	err := pgx.BeginTxFunc(ctx, r.db,
		pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly},
		func(tx pgx.Tx) error {
			const statsQ = `SELECT ballots_total, excluded_nets FROM poll_stats_adjusted WHERE poll_id = $1`
			var raw []byte
			if err := tx.QueryRow(ctx, statsQ, pollID).Scan(&agg.Ballots, &raw); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return ErrNotFound
				}
				return err
			}
			if err := json.Unmarshal(raw, &nets); err != nil {
				return fmt.Errorf("разбор исключённых подсетей: %w", err)
			}
			return readVotes(ctx, tx, "poll_results_adjusted", pollID, &agg)
		})
	if err != nil {
		return domain.Aggregate{}, nil, fmt.Errorf(
			"postgres: чтение публикуемого результата опроса %s: %w", pollID, err)
	}
	if agg.Votes == nil {
		agg.Votes = make(map[uint8]int64)
	}
	if nets == nil {
		nets = []string{}
	}
	return agg, nets, nil
}

// aggregate читает счётчики и бюллетени из указанной пары таблиц ОДНИМ снимком.
//
// Транзакция repeatable read здесь не про долговечность, а про согласованность
// знаменателя: при чтении двумя отдельными запросами между ними успевает
// пройти тик снапшотера, и агрегат получается из двух разных моментов. В одну
// сторону это даёт долю больше 100 % — цифру, которую нельзя показать в эфире.
func (r *ResultRepo) aggregate(
	ctx context.Context, pollID uuid.UUID, resultsTable, statsTable string,
) (domain.Aggregate, error) {
	agg := domain.Aggregate{Votes: make(map[uint8]int64)}

	err := pgx.BeginTxFunc(ctx, r.db,
		pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly},
		func(tx pgx.Tx) error {
			// Имена таблиц — константы этого пакета, не пользовательский ввод.
			// Параметризовать имя таблицы SQL не позволяет; данные во всех
			// запросах передаются только параметрами.
			statsQ := `SELECT ballots_total FROM ` + statsTable + ` WHERE poll_id = $1`
			if err := tx.QueryRow(ctx, statsQ, pollID).Scan(&agg.Ballots); err != nil {
				if !errors.Is(err, pgx.ErrNoRows) {
					return err
				}
				agg.Ballots = 0
			}
			return readVotes(ctx, tx, resultsTable, pollID, &agg)
		})
	if err != nil {
		return domain.Aggregate{}, err
	}
	return agg, nil
}

// readVotes добирает счётчики опций в агрегат.
func readVotes(
	ctx context.Context, tx pgx.Tx, table string, pollID uuid.UUID, agg *domain.Aggregate,
) error {
	q := `SELECT option_idx, votes FROM ` + table + ` WHERE poll_id = $1 ORDER BY option_idx`

	rows, err := tx.Query(ctx, q, pollID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			idx   int16
			votes int64
		)
		if err := rows.Scan(&idx, &votes); err != nil {
			return err
		}
		if idx < 0 || idx > math.MaxUint8 {
			return fmt.Errorf("опция %d опроса %s вне uint8", idx, pollID)
		}
		agg.Add(uint8(idx), votes)
	}
	return rows.Err()
}

// isForeignKeyViolation сообщает, что запись сослалась на несуществующий опрос.
func isForeignKeyViolation(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == "23503"
}
