CREATE TABLE channels (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL,
    content_pool TEXT NOT NULL DEFAULT '{}',
    schedule TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'created' CHECK (status IN ('created', 'running', 'stopped', 'error')),
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE videos (
    id TEXT PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    duration REAL NOT NULL DEFAULT 0,
    tags TEXT NOT NULL DEFAULT '[]',
    processed INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE ad_breaks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id TEXT NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    position REAL NOT NULL CHECK (position >= 0)
);

CREATE INDEX idx_ad_breaks_video_id ON ad_breaks(video_id);
