CREATE TABLE IF NOT EXISTS conversations (
  id uuid PRIMARY KEY,
  title text NOT NULL DEFAULT 'New chat',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS runs (
  id uuid PRIMARY KEY,
  conversation_id uuid NOT NULL REFERENCES conversations(id),
  status text NOT NULL CHECK (status IN ('queued','streaming','done','failed','cancelled','interrupted')),
  created_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz
);
CREATE TABLE IF NOT EXISTS messages (
  seq bigserial PRIMARY KEY,
  id uuid UNIQUE NOT NULL,
  conversation_id uuid NOT NULL REFERENCES conversations(id),
  run_id uuid NOT NULL REFERENCES runs(id),
  role text NOT NULL CHECK (role IN ('user','assistant')),
  content text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (run_id, role)
);
CREATE INDEX IF NOT EXISTS messages_conversation_seq ON messages(conversation_id, seq);
CREATE INDEX IF NOT EXISTS conversations_updated ON conversations(updated_at DESC, id);

CREATE TABLE IF NOT EXISTS sandbox_usage (
  run_id uuid PRIMARY KEY REFERENCES runs(id),
  reserved_seconds integer NOT NULL CHECK (reserved_seconds BETWEEN 0 AND 180),
  sandbox_id text,
  stopped boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS tool_steps (
  run_id uuid NOT NULL REFERENCES runs(id),
  step integer NOT NULL,
  name text NOT NULL,
  arguments jsonb NOT NULL,
  result text,
  status text NOT NULL,
  PRIMARY KEY (run_id, step)
);
CREATE TABLE IF NOT EXISTS artifacts (
  id uuid PRIMARY KEY,
  conversation_id uuid NOT NULL REFERENCES conversations(id),
  run_id uuid REFERENCES runs(id),
  name text NOT NULL,
  mime_type text NOT NULL,
  data bytea NOT NULL CHECK (octet_length(data) <= 2097152),
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS artifacts_conversation ON artifacts(conversation_id, created_at);
