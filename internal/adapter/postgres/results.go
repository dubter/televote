package postgres

import (
	"context"
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

func NewResultRepo(db *DB) (*ResultRepo, error) {
	if db == nil {
		return nil, errors.New("postgres: ResultRepo without a connection pool")
	}
	return &ResultRepo{db: db.pool}, nil
}

func voteArrays(pollID uuid.UUID, a domain.Aggregate) (idx []int16, votes []int64, err error) {
	if a.Ballots < 0 {
		return nil, nil, fmt.Errorf("postgres: poll %s: negative ballot count %d", pollID, a.Ballots)
	}

	indexes := sortedIndexes(a.Votes)
	idx = make([]int16, 0, len(indexes))
	votes = make([]int64, 0, len(indexes))
	for _, i := range indexes {
		v := a.Votes[i]
		if v < 0 {
			return nil, nil, fmt.Errorf("postgres: poll %s, option %d: negative counter %d", pollID, i, v)
		}
		idx = append(idx, int16(i))
		votes = append(votes, v)
	}
	return idx, votes, nil
}

func (r *ResultRepo) Upsert(ctx context.Context, pollID uuid.UUID, a domain.Aggregate) error {
	idx, votes, err := voteArrays(pollID, a)
	if err != nil {
		return err
	}

	err = pgx.BeginFunc(ctx, r.db, func(tx pgx.Tx) error {
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
			return fmt.Errorf("postgres: poll %s: %w", pollID, ErrNotFound)
		}
		return fmt.Errorf("postgres: write result snapshot of poll %s: %w", pollID, err)
	}
	return nil
}

func (r *ResultRepo) Get(ctx context.Context, pollID uuid.UUID) (domain.Aggregate, error) {
	a, err := r.aggregate(ctx, pollID, "poll_results", "poll_stats")
	if err != nil {
		return domain.Aggregate{}, fmt.Errorf("postgres: read results of poll %s: %w", pollID, err)
	}
	return a, nil
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
			return fmt.Errorf("option %d of poll %s is out of uint8", idx, pollID)
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
