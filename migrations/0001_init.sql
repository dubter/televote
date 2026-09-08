-- +goose Up

CREATE TABLE polls (
    id                            uuid             NOT NULL,
    slug                          text             NOT NULL,
    question                      text             NOT NULL,
    type                          text             NOT NULL,
    min_choices                   smallint         NOT NULL,
    max_choices                   smallint         NOT NULL,
    status                        text             NOT NULL,
    opens_at                      timestamptz      NOT NULL,
    closes_at                     timestamptz      NOT NULL,
    shard_count                   integer          NOT NULL,
    expected_audience             bigint           NOT NULL DEFAULT 0,
    expected_conversion           double precision NOT NULL DEFAULT 0.30,
    salt                          bytea            NOT NULL,
    version                       bigint           NOT NULL DEFAULT 1,
    created_at                    timestamptz      NOT NULL DEFAULT now(),

    CONSTRAINT polls_pkey     PRIMARY KEY (id),
    CONSTRAINT polls_slug_key UNIQUE (slug)
);

CREATE INDEX polls_active_idx ON polls (opens_at)
    WHERE status IN ('scheduled', 'open');

CREATE TABLE poll_options (
    poll_id uuid     NOT NULL,
    idx     smallint NOT NULL,
    text    text     NOT NULL,

    CONSTRAINT poll_options_pkey      PRIMARY KEY (poll_id, idx),
    CONSTRAINT poll_options_poll_fkey FOREIGN KEY (poll_id) REFERENCES polls (id) ON DELETE CASCADE
);

CREATE TABLE poll_results (
    poll_id    uuid        NOT NULL,
    option_idx smallint    NOT NULL,
    votes      bigint      NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT poll_results_pkey      PRIMARY KEY (poll_id, option_idx),
    CONSTRAINT poll_results_poll_fkey FOREIGN KEY (poll_id) REFERENCES polls (id) ON DELETE CASCADE
);

CREATE TABLE poll_stats (
    poll_id       uuid        NOT NULL,
    ballots_total bigint      NOT NULL DEFAULT 0,
    updated_at    timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT poll_stats_pkey      PRIMARY KEY (poll_id),
    CONSTRAINT poll_stats_poll_fkey FOREIGN KEY (poll_id) REFERENCES polls (id) ON DELETE CASCADE
);

CREATE TABLE admin_users (
    id            uuid        NOT NULL,
    login         text        NOT NULL,
    password_hash text        NOT NULL,
    role          text        NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT admin_users_pkey      PRIMARY KEY (id),
    CONSTRAINT admin_users_login_key UNIQUE (login)
);

CREATE TABLE admin_audit (
    id      bigint      GENERATED ALWAYS AS IDENTITY,
    actor   text        NOT NULL,
    action  text        NOT NULL,
    entity  text        NOT NULL,
    payload jsonb       NOT NULL DEFAULT '{}'::jsonb,
    at      timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT admin_audit_pkey PRIMARY KEY (id)
);

-- +goose Down

DROP TABLE IF EXISTS admin_audit;
DROP TABLE IF EXISTS admin_users;
DROP TABLE IF EXISTS poll_stats;
DROP TABLE IF EXISTS poll_results;
DROP TABLE IF EXISTS poll_options;
DROP TABLE IF EXISTS polls;
