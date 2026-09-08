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

func NewPollRepo(db *pgxpool.Pool) (*PollRepo, error) {
	if db == nil {
		return nil, errors.New("postgres: PollRepo without a connection pool")
	}
	return &PollRepo{db: db}, nil
}

const pollColumns = `p.id, p.slug, p.question, p.type, p.min_choices, p.max_choices,
	p.status, p.opens_at, p.closes_at, p.shard_count,
	p.expected_audience, p.expected_conversion, p.salt,
	p.version`

const (
	predPollByID   = `p.id = $1`
	predPollBySlug = `p.slug = $1`

	predPollActive = `p.status IN ('scheduled', 'open')`
	predPollAny    = `TRUE`
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
	return r.one(ctx, predPollBySlug, slug)
}

func (r *PollRepo) ListActive(ctx context.Context) ([]*domain.Poll, error) {
	return r.many(ctx, predPollActive)
}

func (r *PollRepo) List(ctx context.Context) ([]*domain.Poll, error) {
	return r.many(ctx, predPollAny)
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

func (r *PollRepo) one(ctx context.Context, pred string, args ...any) (*domain.Poll, error) {
	rows, err := r.many(ctx, pred, args...)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("postgres: poll by predicate %q: %w", pred, ErrNotFound)
	}
	return rows[0], nil
}

func (r *PollRepo) many(ctx context.Context, pred string, args ...any) ([]*domain.Poll, error) {
	q := `SELECT ` + pollColumns + ` FROM polls p WHERE ` + pred + ` ORDER BY p.opens_at, p.id`

	rows, err := r.db.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: select polls (%s): %w", pred, err)
	}
	byID := make(map[uuid.UUID]*domain.Poll)
	out := make([]*domain.Poll, 0, 8)
	for rows.Next() {
		row, err := scanPoll(rows)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: scan poll row: %w", err)
		}
		out = append(out, row)
		byID[row.ID] = row
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: select polls (%s): %w", pred, err)
	}
	if len(out) == 0 {
		return nil, nil
	}

	optQ := `SELECT o.poll_id, o.idx, o.text FROM poll_options o
		WHERE o.poll_id IN (SELECT p.id FROM polls p WHERE ` + pred + `)
		ORDER BY o.poll_id, o.idx`

	optRows, err := r.db.Query(ctx, optQ, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: select options (%s): %w", pred, err)
	}
	defer optRows.Close()
	for optRows.Next() {
		var (
			pollID uuid.UUID
			idx    int16
			text   string
		)
		if err := optRows.Scan(&pollID, &idx, &text); err != nil {
			return nil, fmt.Errorf("postgres: scan option: %w", err)
		}
		row, ok := byID[pollID]
		if !ok {
			continue
		}
		if idx < 0 || idx > domain.MaxOptions {
			return nil, fmt.Errorf("postgres: option %d of poll %s is out of range 0..%d",
				idx, pollID, domain.MaxOptions)
		}
		row.Options = append(row.Options, domain.Option{Idx: uint8(idx), Text: text})
	}
	if err := optRows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: select options (%s): %w", pred, err)
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
		&version,
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
	return &row, nil
}

func sortedIndexes(votes map[uint8]int64) []uint8 {
	return slices.Sorted(maps.Keys(votes))
}
