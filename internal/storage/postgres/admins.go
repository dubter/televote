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

// Admin — администратор из таблицы admin_users.
//
// Role остаётся строкой, а не типом из internal/auth: адаптер хранилища не
// должен зависеть от пакета аутентификации ради одного перечисления. Приведение
// на стороне вызывающего тривиально, а обратный импорт связал бы схему БД с
// форматом токена.
type Admin struct {
	ID    uuid.UUID
	Login string
	// PasswordHash — argon2id-хэш. В логи не попадает никогда.
	PasswordHash string
	Role         string
	CreatedAt    time.Time
}

// AuditEntry — запись журнала действий администратора.
type AuditEntry struct {
	ID     int64
	Actor  string
	Action string
	Entity string
	// Payload — сырой JSON, как он лежит в базе. Разбирать его — дело
	// вызывающего: у разных действий разная форма, и общая структура здесь
	// свелась бы к map[string]any.
	Payload []byte
	At      time.Time
}

// AdminRepo — администраторы и журнал их действий.
type AdminRepo struct {
	db *pgxpool.Pool
}

// NewAdminRepo создаёт репозиторий администраторов.
func NewAdminRepo(db *pgxpool.Pool) (*AdminRepo, error) {
	if db == nil {
		return nil, errors.New("postgres: AdminRepo без пула соединений")
	}
	return &AdminRepo{db: db}, nil
}

// ByLogin читает администратора по логину.
//
// Возвращает ErrNotFound для несуществующего логина. Вызывающий обязан
// потратить то же время на проверку пароля и для неизвестного логина тоже,
// иначе разница во времени ответа превращает форму входа в список логинов.
func (r *AdminRepo) ByLogin(ctx context.Context, login string) (*Admin, error) {
	const q = `
		SELECT id, login, password_hash, role, created_at
		FROM admin_users WHERE login = $1`

	var a Admin
	err := r.db.QueryRow(ctx, q, login).Scan(&a.ID, &a.Login, &a.PasswordHash, &a.Role, &a.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Логин в текст ошибки не подставляется: ошибка попадёт в лог, а
			// перебор логинов через логи — та же утечка, только отложенная.
			return nil, fmt.Errorf("postgres: администратор: %w", ErrNotFound)
		}
		return nil, fmt.Errorf("postgres: чтение администратора: %w", err)
	}
	return &a, nil
}

// EnsureAdmin создаёт администратора, если логин ещё не занят, и сообщает,
// создал ли.
//
// Идемпотентность нужна для бутстрапа: сервис поднимается на пустой базе, и
// первый администратор должен появиться сам, иначе админкой нельзя
// воспользоваться. При повторном старте пароль НЕ перезаписывается — иначе
// каждый рестарт откатывал бы смену пароля к значению из переменной окружения.
func (r *AdminRepo) EnsureAdmin(ctx context.Context, a Admin) (bool, error) {
	if a.Login == "" || a.PasswordHash == "" || a.Role == "" {
		return false, errors.New("postgres: EnsureAdmin: логин, хэш и роль обязательны")
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
			// DO NOTHING не вернул строку — администратор уже был.
			return false, nil
		}
		return false, fmt.Errorf("postgres: создание администратора: %w", err)
	}
	return true, nil
}

// Audit записывает действие администратора в журнал.
//
// Ошибку записи нельзя глотать: журнал — единственный след того, кто продлил
// окно голосования или исключил подсеть, и действие без записи в нём
// неотличимо от того, которого не было.
//
// В payload кладётся конфигурация опроса и параметры действия — и никогда
// данные голосующих: ни voter_id, ни полный IP, ни содержимое токена
// (CLAUDE.md, «Приватность»). Тип any здесь — про форму, а не про свободу:
// проверить содержимое кодом нельзя, поэтому ответственность на вызывающем.
func (r *AdminRepo) Audit(ctx context.Context, actor, action, entity string, payload any) error {
	if actor == "" || action == "" || entity == "" {
		return errors.New("postgres: Audit: actor, action и entity обязательны")
	}

	raw := []byte("{}")
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("postgres: сериализация payload аудита: %w", err)
		}
		raw = encoded
	}

	const q = `
		INSERT INTO admin_audit (actor, action, entity, payload, at)
		VALUES ($1, $2, $3, $4::jsonb, now())`

	if _, err := r.db.Exec(ctx, q, actor, action, entity, string(raw)); err != nil {
		return fmt.Errorf("postgres: запись аудита %q/%q: %w", action, entity, err)
	}
	return nil
}

// ListAudit читает последние записи журнала по сущности, новые первыми.
//
// Пустой entity означает «по всем сущностям». Лимит обязателен и ограничен
// сверху: журнал растёт неограниченно, и запрос без предела однажды вытянет
// его целиком в память админки.
func (r *AdminRepo) ListAudit(ctx context.Context, entity string, limit int) ([]AuditEntry, error) {
	const maxLimit = 1000
	if limit <= 0 || limit > maxLimit {
		limit = maxLimit
	}

	// Фильтр выражен как «$1 = '' OR entity = $1», чтобы запрос был один и
	// параметризованный: склейка условий строкой — это тот же путь, которым в
	// код попадает инъекция.
	const q = `
		SELECT id, actor, action, entity, payload, at
		FROM admin_audit
		WHERE $1 = '' OR entity = $1
		ORDER BY at DESC, id DESC
		LIMIT $2`

	rows, err := r.db.Query(ctx, q, entity, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: чтение аудита: %w", err)
	}
	defer rows.Close()

	out := make([]AuditEntry, 0, 16)
	for rows.Next() {
		var e AuditEntry
		if err := rows.Scan(&e.ID, &e.Actor, &e.Action, &e.Entity, &e.Payload, &e.At); err != nil {
			return nil, fmt.Errorf("postgres: разбор записи аудита: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: чтение аудита: %w", err)
	}
	return out, nil
}
