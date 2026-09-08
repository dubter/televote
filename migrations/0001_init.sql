-- +goose Up

CREATE TABLE polls (
    id                            uuid        NOT NULL,
    slug                          text        NOT NULL,
    question                      text        NOT NULL,
    type                          text        NOT NULL,
    min_choices                   smallint    NOT NULL,
    max_choices                   smallint    NOT NULL,
    status                        text        NOT NULL,
    opens_at                      timestamptz NOT NULL,
    closes_at                     timestamptz NOT NULL,
    shard_count                   integer     NOT NULL,
    expected_audience             bigint      NOT NULL DEFAULT 0,
    expected_conversion           double precision NOT NULL DEFAULT 0.30,
    salt                          bytea       NOT NULL,
    results_visible_during_voting boolean     NOT NULL DEFAULT false,
    version                       bigint      NOT NULL DEFAULT 1,
    created_at                    timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT polls_pkey            PRIMARY KEY (id),
    CONSTRAINT polls_slug_key        UNIQUE (slug),
    CONSTRAINT polls_slug_shape      CHECK (slug ~ '^[a-z0-9][a-z0-9_-]{0,63}$'),
    CONSTRAINT polls_question_len    CHECK (length(question) BETWEEN 1 AND 1024),
    CONSTRAINT polls_type_known      CHECK (type IN ('single', 'multiple')),
    CONSTRAINT polls_status_known    CHECK (status IN ('draft', 'scheduled', 'open', 'closed', 'archived')),
    CONSTRAINT polls_choices_sane    CHECK (min_choices >= 1 AND max_choices >= min_choices AND max_choices <= 256),
    CONSTRAINT polls_window_sane     CHECK (closes_at > opens_at),
    CONSTRAINT polls_shard_count_cap CHECK (shard_count BETWEEN 1 AND 16384),
    CONSTRAINT polls_version_positive CHECK (version >= 1),
    CONSTRAINT polls_audience_nonneg  CHECK (expected_audience >= 0),
    CONSTRAINT polls_conversion_frac  CHECK (expected_conversion >= 0 AND expected_conversion <= 1),
    CONSTRAINT polls_salt_len         CHECK (length(salt) = 32)
);

CREATE INDEX polls_active_idx ON polls (opens_at)
    WHERE status IN ('scheduled', 'open');

CREATE TABLE poll_options (
    poll_id uuid     NOT NULL,
    idx     smallint NOT NULL,
    text    text     NOT NULL,

    CONSTRAINT poll_options_pkey       PRIMARY KEY (poll_id, idx),
    CONSTRAINT poll_options_poll_fkey  FOREIGN KEY (poll_id) REFERENCES polls (id) ON DELETE CASCADE,
    CONSTRAINT poll_options_idx_range  CHECK (idx BETWEEN 0 AND 255),
    CONSTRAINT poll_options_text_len   CHECK (length(text) BETWEEN 1 AND 512)
);

CREATE TABLE poll_results (
    poll_id    uuid        NOT NULL,
    option_idx smallint    NOT NULL,
    votes      bigint      NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT poll_results_pkey        PRIMARY KEY (poll_id, option_idx),
    CONSTRAINT poll_results_poll_fkey   FOREIGN KEY (poll_id) REFERENCES polls (id) ON DELETE CASCADE,
    CONSTRAINT poll_results_idx_range   CHECK (option_idx BETWEEN 0 AND 255),
    CONSTRAINT poll_results_votes_nonneg CHECK (votes >= 0)
);

CREATE TABLE poll_stats (
    poll_id       uuid        NOT NULL,
    ballots_total bigint      NOT NULL DEFAULT 0,
    updated_at    timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT poll_stats_pkey            PRIMARY KEY (poll_id),
    CONSTRAINT poll_stats_poll_fkey       FOREIGN KEY (poll_id) REFERENCES polls (id) ON DELETE CASCADE,
    CONSTRAINT poll_stats_ballots_nonneg  CHECK (ballots_total >= 0)
);

CREATE TABLE poll_results_adjusted (
    poll_id    uuid     NOT NULL,
    option_idx smallint NOT NULL,
    votes      bigint   NOT NULL DEFAULT 0,

    CONSTRAINT poll_results_adjusted_pkey PRIMARY KEY (poll_id, option_idx),
    CONSTRAINT poll_results_adjusted_poll_fkey FOREIGN KEY (poll_id)
        REFERENCES polls (id) ON DELETE CASCADE,
    CONSTRAINT poll_results_adjusted_idx_range CHECK (option_idx BETWEEN 0 AND 255),
    CONSTRAINT poll_results_adjusted_votes_nonneg CHECK (votes >= 0)
);

CREATE TABLE poll_stats_adjusted (
    poll_id       uuid        NOT NULL,
    ballots_total bigint      NOT NULL DEFAULT 0,
    excluded_nets jsonb       NOT NULL DEFAULT '[]'::jsonb,
    at            timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT poll_stats_adjusted_pkey PRIMARY KEY (poll_id),
    CONSTRAINT poll_stats_adjusted_poll_fkey FOREIGN KEY (poll_id)
        REFERENCES polls (id) ON DELETE CASCADE,
    CONSTRAINT poll_stats_adjusted_ballots_nonneg CHECK (ballots_total >= 0),
    CONSTRAINT poll_stats_adjusted_nets_is_array  CHECK (jsonb_typeof(excluded_nets) = 'array')
);

CREATE TABLE admin_users (
    id            uuid        NOT NULL,
    login         text        NOT NULL,
    password_hash text        NOT NULL,
    role          text        NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT admin_users_pkey       PRIMARY KEY (id),
    CONSTRAINT admin_users_login_key  UNIQUE (login),
    CONSTRAINT admin_users_login_len  CHECK (length(login) BETWEEN 3 AND 64),
    CONSTRAINT admin_users_role_known CHECK (role IN ('admin', 'editor', 'viewer'))
);

CREATE TABLE admin_audit (
    id      bigint      GENERATED ALWAYS AS IDENTITY,
    actor   text        NOT NULL,
    action  text        NOT NULL,
    entity  text        NOT NULL,
    payload jsonb       NOT NULL DEFAULT '{}'::jsonb,
    at      timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT admin_audit_pkey       PRIMARY KEY (id),
    CONSTRAINT admin_audit_actor_len  CHECK (length(actor) BETWEEN 1 AND 128),
    CONSTRAINT admin_audit_action_len CHECK (length(action) BETWEEN 1 AND 64),
    CONSTRAINT admin_audit_entity_len CHECK (length(entity) BETWEEN 1 AND 128)
);

CREATE INDEX admin_audit_at_idx     ON admin_audit (at DESC, id DESC);
CREATE INDEX admin_audit_entity_idx ON admin_audit (entity, at DESC);

-- +goose Down

DROP TABLE IF EXISTS admin_audit;
DROP TABLE IF EXISTS admin_users;
DROP TABLE IF EXISTS poll_stats_adjusted;
DROP TABLE IF EXISTS poll_results_adjusted;
DROP TABLE IF EXISTS poll_stats;
DROP TABLE IF EXISTS poll_results;
DROP TABLE IF EXISTS poll_options;
DROP TABLE IF EXISTS polls;
