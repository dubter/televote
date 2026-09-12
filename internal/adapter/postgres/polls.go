package postgres

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dubter/televote/internal/domain"
)

const SaltLen = 32

type PollRepo struct {
	db *pgxpool.Pool
}

func NewPollRepo(db *DB) (*PollRepo, error) {
	if db == nil {
		return nil, errors.New("postgres: PollRepo without a connection pool")
	}
	return &PollRepo{db: db.pool}, nil
}

const selectPolls = `
	SELECT p.id, p.slug, p.question, p.type, p.min_choices, p.max_choices,
	       p.status, p.opens_at, p.closes_at, p.shard_count,
	       p.expected_audience, p.expected_conversion, p.salt, p.version,
	       COALESCE(array_agg(o.idx  ORDER BY o.idx) FILTER (WHERE o.idx IS NOT NULL), '{}'),
	       COALESCE(array_agg(o.text ORDER BY o.idx) FILTER (WHERE o.idx IS NOT NULL), '{}')
	FROM polls p
	LEFT JOIN poll_options o ON o.poll_id = p.id`

const (
	pollBySlug = selectPolls + `
	WHERE p.slug = $1
	GROUP BY p.id`

	activePolls = selectPolls + `
	WHERE p.status IN ('scheduled', 'open')
	GROUP BY p.id
	ORDER BY p.opens_at, p.id`

	allPolls = selectPolls + `
	GROUP BY p.id
	ORDER BY p.opens_at, p.id`
)

func (r *PollRepo) Create(ctx context.Context, row *domain.Poll) error {
	if row == nil {
		return errors.New("postgres: CreateRow without a poll")
	}
	if len(row.Options) == 0 {
		return fmt.Errorf("postgres: poll %q has no options: %w", row.Slug, domain.ErrInvalidChoices)
	}
	if len(row.Options) > domain.MaxOptions {
		return fmt.Errorf("postgres: poll %q has %d options, limit is %d: %w",
			row.Slug, len(row.Options), domain.MaxOptions, domain.ErrInvalidChoices)
	}

	salt := make([]byte, SaltLen)
	if _, err := rand.Read(salt); err != nil {
		return fmt.Errorf("postgres: generate poll salt: %w", err)
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
			                   version)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`
		if _, err := tx.Exec(ctx, insertPoll,
			row.ID, row.Slug, row.Question, string(row.Type),
			int16(row.MinChoices), int16(row.MaxChoices), string(row.Status),
			row.OpensAt, row.ClosesAt, int32(row.ShardCount),
			row.ExpectedAudience, row.ExpectedConversion, salt,
			int64(version),
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
			return fmt.Errorf("postgres: slug %q: %w", row.Slug, ErrSlugTaken)
		}
		return fmt.Errorf("postgres: create poll %q: %w", row.Slug, err)
	}

	row.Salt = salt
	row.Version = version
	return nil
}

func (r *PollRepo) GetBySlug(ctx context.Context, slug string) (*domain.Poll, error) {
	polls, err := r.query(ctx, pollBySlug, slug)
	if err != nil {
		return nil, err
	}
	if len(polls) == 0 {
		return nil, fmt.Errorf("postgres: poll %q: %w", slug, ErrNotFound)
	}
	return polls[0], nil
}

func (r *PollRepo) ListActive(ctx context.Context) ([]*domain.Poll, error) {
	return r.query(ctx, activePolls)
}

func (r *PollRepo) List(ctx context.Context) ([]*domain.Poll, error) {
	return r.query(ctx, allPolls)
}

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
		return fmt.Errorf("postgres: close poll window %s: %w", id, err)
	}
	return versionedOutcome(id, version, applied, exists)
}

func (r *PollRepo) Transition(ctx context.Context, id uuid.UUID, to domain.Status, version uint32) error {
	if !to.Valid() {
		return fmt.Errorf("postgres: unknown status %q: %w", to, domain.ErrBadTransition)
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
		return fmt.Errorf("postgres: transition poll %s to %q: %w", id, to, err)
	}
	return versionedOutcome(id, version, applied, exists)
}

func versionedOutcome(id uuid.UUID, version uint32, applied, exists bool) error {
	switch {
	case applied:
		return nil
	case !exists:
		return fmt.Errorf("postgres: poll %s: %w", id, ErrNotFound)
	default:
		return fmt.Errorf("postgres: poll %s, version %d: %w", id, version, ErrVersionConflict)
	}
}

func (r *PollRepo) query(ctx context.Context, sql string, args ...any) ([]*domain.Poll, error) {
	rows, err := r.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: select polls: %w", err)
	}
	defer rows.Close()

	var out []*domain.Poll
	for rows.Next() {
		poll, err := scanPoll(rows)
		if err != nil {
			return nil, fmt.Errorf("postgres: scan poll: %w", err)
		}
		out = append(out, poll)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: select polls: %w", err)
	}
	return out, nil
}

func scanPoll(rows pgx.Rows) (*domain.Poll, error) {
	var (
		row         domain.Poll
		pollType    string
		status      string
		minChoices  int16
		maxChoices  int16
		shardCount  int32
		version     int64
		optionIdx   []int16
		optionTexts []string
	)

	if err := rows.Scan(
		&row.ID, &row.Slug, &row.Question, &pollType, &minChoices, &maxChoices,
		&status, &row.OpensAt, &row.ClosesAt, &shardCount,
		&row.ExpectedAudience, &row.ExpectedConversion, &row.Salt, &version,
		&optionIdx, &optionTexts,
	); err != nil {
		return nil, err
	}

	row.Type = domain.PollType(pollType)
	if !row.Type.Valid() {
		return nil, fmt.Errorf("poll %s: unknown type %q", row.ID, pollType)
	}
	row.Status = domain.Status(status)
	if !row.Status.Valid() {
		return nil, fmt.Errorf("poll %s: unknown status %q", row.ID, status)
	}
	if minChoices < 0 || minChoices > math.MaxUint8 || maxChoices < 0 || maxChoices > math.MaxUint8 {
		return nil, fmt.Errorf("poll %s: min/max choices %d/%d out of uint8", row.ID, minChoices, maxChoices)
	}
	if shardCount < 1 || shardCount > domain.MaxShardCount {
		return nil, fmt.Errorf("poll %s: shard_count %d out of 1..%d", row.ID, shardCount, domain.MaxShardCount)
	}
	if version < 1 || version > math.MaxUint32 {
		return nil, fmt.Errorf("poll %s: version %d out of uint32", row.ID, version)
	}

	row.MinChoices = uint8(minChoices)
	row.MaxChoices = uint8(maxChoices)
	row.ShardCount = uint16(shardCount)
	row.Version = uint32(version)

	row.Options = make([]domain.Option, 0, len(optionIdx))
	for i, idx := range optionIdx {
		if idx < 0 || idx > domain.MaxOptions {
			return nil, fmt.Errorf("poll %s: option %d is out of range 0..%d", row.ID, idx, domain.MaxOptions)
		}
		row.Options = append(row.Options, domain.Option{Idx: uint8(idx), Text: optionTexts[i]})
	}
	return &row, nil
}

func sortedIndexes(votes map[uint8]int64) []uint8 {
	return slices.Sorted(maps.Keys(votes))
}
