package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Admin struct {
	ID           uuid.UUID
	Login        string
	PasswordHash string
	Role         string
	CreatedAt    time.Time
}

type AuditEntry struct {
	ID      int64
	Actor   string
	Action  string
	Entity  string
	Payload []byte
	At      time.Time
}

type AdminRepo struct {
	db *pgxpool.Pool
}

func NewAdminRepo(db *pgxpool.Pool) (*AdminRepo, error) {
	if db == nil {
		return nil, errors.New("postgres: AdminRepo without a connection pool")
	}
	return &AdminRepo{db: db}, nil
}

func (r *AdminRepo) ByLogin(ctx context.Context, login string) (*Admin, error) {
	const q = `
		SELECT id, login, password_hash, role, created_at
		FROM admin_users WHERE login = $1`

	var a Admin
	err := r.db.QueryRow(ctx, q, login).Scan(&a.ID, &a.Login, &a.PasswordHash, &a.Role, &a.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("postgres: admin: %w", ErrNotFound)
		}
		return nil, fmt.Errorf("postgres: read admin: %w", err)
	}
	return &a, nil
}

func (r *AdminRepo) EnsureAdmin(ctx context.Context, a Admin) (bool, error) {
	if a.Login == "" || a.PasswordHash == "" || a.Role == "" {
		return false, errors.New("postgres: EnsureAdmin: login, hash and role are required")
	}
	id := a.ID
	if id == uuid.Nil {
		id = uuid.New()
	}

	const q = `
		INSERT INTO admin_users (id, login, password_hash, role)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (login) DO NOTHING
		RETURNING id`

	var created uuid.UUID
	err := r.db.QueryRow(ctx, q, id, a.Login, a.PasswordHash, a.Role).Scan(&created)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("postgres: create admin: %w", err)
	}
	return true, nil
}

func (r *AdminRepo) Audit(ctx context.Context, actor, action, entity string, payload any) error {
	if actor == "" || action == "" || entity == "" {
		return errors.New("postgres: Audit: actor, action and entity are required")
	}

	raw := []byte("{}")
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("postgres: marshal audit payload: %w", err)
		}
		raw = encoded
	}

	const q = `
		INSERT INTO admin_audit (actor, action, entity, payload, at)
		VALUES ($1, $2, $3, $4::jsonb, now())`

	if _, err := r.db.Exec(ctx, q, actor, action, entity, string(raw)); err != nil {
		return fmt.Errorf("postgres: write audit %q/%q: %w", action, entity, err)
	}
	return nil
}

func (r *AdminRepo) ListAudit(ctx context.Context, entity string, limit int) ([]AuditEntry, error) {
	const maxLimit = 1000
	if limit <= 0 || limit > maxLimit {
		limit = maxLimit
	}

	const q = `
		SELECT id, actor, action, entity, payload, at
		FROM admin_audit
		WHERE $1 = '' OR entity = $1
		ORDER BY at DESC, id DESC
		LIMIT $2`

	rows, err := r.db.Query(ctx, q, entity, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: read audit: %w", err)
	}
	defer rows.Close()

	out := make([]AuditEntry, 0, 16)
	for rows.Next() {
		var e AuditEntry
		if err := rows.Scan(&e.ID, &e.Actor, &e.Action, &e.Entity, &e.Payload, &e.At); err != nil {
			return nil, fmt.Errorf("postgres: scan audit row: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: read audit: %w", err)
	}
	return out, nil
}
