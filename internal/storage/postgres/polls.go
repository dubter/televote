package postgres

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dubter/televote/internal/domain"
)

const SaltLen = 32

type PollRepo struct {
	db *pgxpool.Pool
}

func NewPollRepo(db *pgxpool.Pool) (*PollRepo, error) {
	if db == nil {
		return nil, errors.New("postgres: PollRepo без пула соединений")
	}
	return &PollRepo{db: db}, nil
}

const pollColumns = `p.id, p.slug, p.question, p.type, p.min_choices, p.max_choices,
	p.status, p.opens_at, p.closes_at, p.shard_count,
	p.expected_audience, p.expected_conversion, p.salt,
	p.results_visible_during_voting, p.version`

const (
	predPollByID   = `p.id = $1`
	predPollBySlug = `p.slug = $1`

	predPollActive = `p.status IN ('scheduled', 'open')`
	predPollAny    = `TRUE`
)

func (r *PollRepo) Create(ctx context.Context, row *domain.Poll) error {
	if row == nil {
		return errors.New("postgres: CreateRow без опроса")
	}
	if len(row.Options) == 0 {
		return fmt.Errorf("postgres: опрос %q без опций: %w", row.Slug, domain.ErrInvalidChoices)
	}
	if len(row.Options) > domain.MaxOptions {
		return fmt.Errorf("postgres: у опроса %q %d опций, предел %d: %w",
			row.Slug, len(row.Options), domain.MaxOptions, domain.ErrInvalidChoices)
	}

	salt := make([]byte, SaltLen)
	if _, err := rand.Read(salt); err != nil {
		return fmt.Errorf("postgres: генерация соли опроса: %w", err)
	}

	version := row.Version
	if version == 0 {
		version = 1
	}

	idx := make([]int16, 0, len(row.Options))
	texts := make([]string, 0, len(row.Options))
	for _, o := range row.Options {
		idx = append(idx, int16(o.Idx))
		texts = append(texts, o.Text)
	}

	err := pgx.BeginFunc(ctx, r.db, func(tx pgx.Tx) error {
		const insertPoll = `
			INSERT INTO polls (id, slug, question, type, min_choices, max_choices,
			                   status, opens_at, closes_at, shard_count,
			                   expected_audience, expected_conversion, salt,
			                   results_visible_during_voting, version)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`
		if _, err := tx.Exec(ctx, insertPoll,
			row.ID, row.Slug, row.Question, string(row.Type),
			int16(row.MinChoices), int16(row.MaxChoices), string(row.Status),
			row.OpensAt, row.ClosesAt, int32(row.ShardCount),
			row.ExpectedAudience, row.ExpectedConversion, salt,
			row.ResultsVisibleDuringVoting, int64(version),
		); err != nil {
			return err
		}

		const insertOptions = `
			INSERT INTO poll_options (poll_id, idx, text)
			SELECT $1, t.idx, t.text FROM unnest($2::smallint[], $3::text[]) AS t(idx, text)`
		_, err := tx.Exec(ctx, insertOptions, row.ID, idx, texts)
		return err
	})
	if err != nil {
		if isUniqueViolation(err, "polls_slug_key") {
			return fmt.Errorf("postgres: слаг %q: %w", row.Slug, ErrSlugTaken)
		}
		return fmt.Errorf("postgres: создание опроса %q: %w", row.Slug, err)
	}

	row.Salt = salt
	row.Version = version
	return nil
}

func (r *PollRepo) GetBySlug(ctx context.Context, slug string) (*domain.Poll, error) {
	return r.one(ctx, predPollBySlug, slug)
}

func (r *PollRepo) ListActive(ctx context.Context) ([]*domain.Poll, error) {
	return r.many(ctx, predPollActive)
}

func (r *PollRepo) List(ctx context.Context) ([]*domain.Poll, error) {
	return r.many(ctx, predPollAny)
}

// Ручное закрытие двигает границу окна, а не статус: статус снимет снапшотер,
// когда дренаж дойдёт до нуля. Прямой перевод в closed выбросил бы опрос из
// выборки снапшотера, и голоса последних секунд не попали бы в результат.
func (r *PollRepo) CloseNow(ctx context.Context, id uuid.UUID, version uint32) error {
	const q = `
		WITH updated AS (
			UPDATE polls SET closes_at = now(), version = version + 1
			WHERE id = $1 AND version = $2
			RETURNING 1
		)
		SELECT EXISTS (SELECT 1 FROM updated), EXISTS (SELECT 1 FROM polls WHERE id = $1)`

	var applied, exists bool
	if err := r.db.QueryRow(ctx, q, id, int64(version)).Scan(&applied, &exists); err != nil {
		return fmt.Errorf("postgres: закрытие окна опроса %s: %w", id, err)
	}

	switch {
	case applied:
		return nil
	case !exists:
		return fmt.Errorf("postgres: опрос %s: %w", id, ErrNotFound)
	default:
		return fmt.Errorf("postgres: опрос %s, версия %d: %w", id, version, ErrVersionConflict)
	}
}

func (r *PollRepo) Transition(ctx context.Context, id uuid.UUID, to domain.Status, version uint32) error {
	if !to.Valid() {
		return fmt.Errorf("postgres: неизвестный статус %q: %w", to, domain.ErrBadTransition)
	}

	const q = `
		WITH updated AS (
			UPDATE polls SET status = $2, version = version + 1
			WHERE id = $1 AND version = $3
			RETURNING 1
		)
		SELECT EXISTS (SELECT 1 FROM updated), EXISTS (SELECT 1 FROM polls WHERE id = $1)`

	var applied, exists bool
	if err := r.db.QueryRow(ctx, q, id, string(to), int64(version)).Scan(&applied, &exists); err != nil {
		return fmt.Errorf("postgres: переход опроса %s в %q: %w", id, to, err)
	}

	switch {
	case applied:
		return nil
	case !exists:
		return fmt.Errorf("postgres: опрос %s: %w", id, ErrNotFound)
	default:
		return fmt.Errorf("postgres: опрос %s, версия %d: %w", id, version, ErrVersionConflict)
	}
}

func (r *PollRepo) HasCountedVotes(ctx context.Context, id uuid.UUID) (bool, error) {
	const q = `
		SELECT EXISTS (SELECT 1 FROM poll_results WHERE poll_id = $1 AND votes > 0)
		    OR EXISTS (SELECT 1 FROM poll_stats   WHERE poll_id = $1 AND ballots_total > 0)`

	var has bool
	if err := r.db.QueryRow(ctx, q, id).Scan(&has); err != nil {
		return false, fmt.Errorf("postgres: проверка голосов опроса %s: %w", id, err)
	}
	return has, nil
}

func (r *PollRepo) one(ctx context.Context, pred string, args ...any) (*domain.Poll, error) {
	rows, err := r.many(ctx, pred, args...)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("postgres: опрос по условию %q: %w", pred, ErrNotFound)
	}
	return rows[0], nil
}

func (r *PollRepo) many(ctx context.Context, pred string, args ...any) ([]*domain.Poll, error) {
	q := `SELECT ` + pollColumns + ` FROM polls p WHERE ` + pred + ` ORDER BY p.opens_at, p.id`

	rows, err := r.db.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: выборка опросов (%s): %w", pred, err)
	}
	byID := make(map[uuid.UUID]*domain.Poll)
	out := make([]*domain.Poll, 0, 8)
	for rows.Next() {
		row, err := scanPoll(rows)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: разбор строки опроса: %w", err)
		}
		out = append(out, row)
		byID[row.ID] = row
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: выборка опросов (%s): %w", pred, err)
	}
	if len(out) == 0 {
		return nil, nil
	}

	optQ := `SELECT o.poll_id, o.idx, o.text FROM poll_options o
		WHERE o.poll_id IN (SELECT p.id FROM polls p WHERE ` + pred + `)
		ORDER BY o.poll_id, o.idx`

	optRows, err := r.db.Query(ctx, optQ, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: выборка опций (%s): %w", pred, err)
	}
	defer optRows.Close()
	for optRows.Next() {
		var (
			pollID uuid.UUID
			idx    int16
			text   string
		)
		if err := optRows.Scan(&pollID, &idx, &text); err != nil {
			return nil, fmt.Errorf("postgres: разбор опции: %w", err)
		}
		row, ok := byID[pollID]
		if !ok {
			continue
		}
		if idx < 0 || idx > domain.MaxOptions {
			return nil, fmt.Errorf("postgres: опция %d опроса %s вне диапазона 0..%d",
				idx, pollID, domain.MaxOptions)
		}
		row.Options = append(row.Options, domain.Option{Idx: uint8(idx), Text: text})
	}
	if err := optRows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: выборка опций (%s): %w", pred, err)
	}

	return out, nil
}

func scanPoll(rows pgx.Rows) (*domain.Poll, error) {
	var (
		row        domain.Poll
		pollType   string
		status     string
		minChoices int16
		maxChoices int16
		shardCount int32
		version    int64
	)

	if err := rows.Scan(
		&row.ID, &row.Slug, &row.Question, &pollType, &minChoices, &maxChoices,
		&status, &row.OpensAt, &row.ClosesAt, &shardCount,
		&row.ExpectedAudience, &row.ExpectedConversion, &row.Salt,
		&row.ResultsVisibleDuringVoting, &version,
	); err != nil {
		return nil, err
	}

	row.Type = domain.PollType(pollType)
	if !row.Type.Valid() {
		return nil, fmt.Errorf("опрос %s: неизвестный тип %q", row.ID, pollType)
	}
	row.Status = domain.Status(status)
	if !row.Status.Valid() {
		return nil, fmt.Errorf("опрос %s: неизвестный статус %q", row.ID, status)
	}
	if minChoices < 0 || minChoices > math.MaxUint8 || maxChoices < 0 || maxChoices > math.MaxUint8 {
		return nil, fmt.Errorf("опрос %s: min/max choices %d/%d вне uint8", row.ID, minChoices, maxChoices)
	}
	if shardCount < 1 || shardCount > domain.MaxShardCount {
		return nil, fmt.Errorf("опрос %s: shard_count %d вне 1..%d", row.ID, shardCount, domain.MaxShardCount)
	}
	if version < 1 || version > math.MaxUint32 {
		return nil, fmt.Errorf("опрос %s: version %d вне uint32", row.ID, version)
	}

	row.MinChoices = uint8(minChoices)
	row.MaxChoices = uint8(maxChoices)
	row.ShardCount = uint16(shardCount)
	row.Version = uint32(version)
	return &row, nil
}

func sortedIndexes(votes map[uint8]int64) []uint8 {
	out := make([]uint8, 0, len(votes))
	for idx := range votes {
		out = append(out, idx)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
