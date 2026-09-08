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

	"github.com/dubter/televote/internal/domain"
)

type ResultRepo struct {
	db *pgxpool.Pool
}

func NewResultRepo(db *pgxpool.Pool) (*ResultRepo, error) {
	if db == nil {
		return nil, errors.New("postgres: ResultRepo без пула соединений")
	}
	return &ResultRepo{db: db}, nil
}

func (r *ResultRepo) Upsert(ctx context.Context, pollID uuid.UUID, a domain.Aggregate) error {
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

func (r *ResultRepo) Get(ctx context.Context, pollID uuid.UUID) (domain.Aggregate, error) {
	a, err := r.aggregate(ctx, pollID, "poll_results", "poll_stats")
	if err != nil {
		return domain.Aggregate{}, fmt.Errorf("postgres: чтение результатов опроса %s: %w", pollID, err)
	}
	return a, nil
}

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

	if excludedNets == nil {
		excludedNets = []string{}
	}
	nets, err := json.Marshal(excludedNets)
	if err != nil {
		return fmt.Errorf("postgres: опрос %s: сериализация исключённых подсетей: %w", pollID, err)
	}

	err = pgx.BeginFunc(ctx, r.db, func(tx pgx.Tx) error {
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

func (r *ResultRepo) GetAdjusted(
	ctx context.Context, pollID uuid.UUID,
) (domain.Aggregate, []string, error) {
	agg := domain.NewAggregate()
	var nets []string

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
	if nets == nil {
		nets = []string{}
	}
	return agg, nets, nil
}

func (r *ResultRepo) aggregate(
	ctx context.Context, pollID uuid.UUID, resultsTable, statsTable string,
) (domain.Aggregate, error) {
	agg := domain.NewAggregate()

	err := pgx.BeginTxFunc(ctx, r.db,
		pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly},
		func(tx pgx.Tx) error {
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

func isForeignKeyViolation(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == "23503"
}
