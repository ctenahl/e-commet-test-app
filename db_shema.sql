CREATE TABLE repos
(
    repo_id bigint NOT NULL,
    repo character varying(100) NOT NULL,
    owner character varying(100) NOT NULL,
    stars bigint NOT NULL,
    watchers bigint NOT NULL,
    forks bigint NOT NULL,
    open_issues bigint NOT NULL,
    language bit varying(100) NOT NULL,
	position_cur bigint NOT NULL SET DEFAULT 0,
    position_prev bigint NOT NULL,
    PRIMARY KEY (repo_id)
);

CREATE TABLE commits
(
    full_name character varying(100) NOT NULL,
    date date NOT NULL,
	commits_count bigint,
    authors character varying(300),
    status boolean NOT NULL DEFAULT False
);

CREATE INDEX ON commits USING btree (date ASC NULLS LAST) WITH (deduplicate_items = True);