-- A source may be created from a known WeRead book ID without first finding
-- a representative public article URL. Keep the URL when supplied, but do
-- not require a synthetic or guessed URL for book-only sources.
ALTER TABLE sources
    ALTER COLUMN article_url DROP NOT NULL;
