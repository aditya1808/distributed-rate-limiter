ALTER TABLE assignments ADD COLUMN id text;
ALTER TABLE assignments ADD COLUMN version bigint NOT NULL DEFAULT 1 CHECK (version > 0);
UPDATE assignments SET id = 'a_' || substr(md5(tenant || '|' || namespace), 1, 26);
ALTER TABLE assignments ALTER COLUMN id SET NOT NULL;
ALTER TABLE assignments ALTER COLUMN version DROP DEFAULT;

ALTER TABLE assignments DROP CONSTRAINT assignments_pkey;
ALTER TABLE assignments ADD PRIMARY KEY (id);
ALTER TABLE assignments ADD CONSTRAINT assignments_tenant_namespace_key UNIQUE (tenant, namespace);
