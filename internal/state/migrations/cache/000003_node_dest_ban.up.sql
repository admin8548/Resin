CREATE TABLE IF NOT EXISTS node_dest_ban (
	node_hash        TEXT NOT NULL,
	domain           TEXT NOT NULL,
	fail_count       INTEGER NOT NULL DEFAULT 0,
	banned_until_ns  INTEGER NOT NULL DEFAULT 0,
	last_error       TEXT NOT NULL DEFAULT '',
	last_fail_at_ns  INTEGER NOT NULL DEFAULT 0,
	last_access_ns   INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (node_hash, domain)
);
