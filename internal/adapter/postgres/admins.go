package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dubter/televote/internal/domain"
)

type AdminRepo struct {
	db *pgxpool.Pool
}

func NewAdminRepo(db *DB) (*AdminRepo, error) {
	if db == nil {
		return nil, errors.New("postgres: AdminRepo without a connection pool")
	}
	return &AdminRepo{db: db.pool}, nil
}

func (r *AdminRepo) ByLogin(ctx context.Context, login string) (*domain.Admin, error) {
	const q = `
		SELECT id, login, password_hash, role
		FROM admin_users WHERE login = $1`

	var a domain.Admin
	err := r.db.QueryRow(ctx, q, login).Scan(&a.ID, &a.Login, &a.PasswordHash, &a.Role)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("postgres: admin: %w", ErrNotFound)
		}
		return nil, fmt.Errorf("postgres: read admin: %w", err)
	}
	return &a, nil
}

func (r *AdminRepo) EnsureAdmin(ctx context.Context, a domain.Admin) (bool, error) {
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
