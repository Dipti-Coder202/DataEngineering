-- Add reliable change-tracking metadata without changing Project 6's columns.
-- This migration is idempotent and safe to run more than once.

BEGIN;

ALTER TABLE public.retail_sales
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;

UPDATE public.retail_sales
SET updated_at = CURRENT_TIMESTAMP
WHERE updated_at IS NULL;

ALTER TABLE public.retail_sales
    ALTER COLUMN updated_at SET DEFAULT CURRENT_TIMESTAMP,
    ALTER COLUMN updated_at SET NOT NULL;

CREATE OR REPLACE FUNCTION public.set_retail_sales_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS retail_sales_set_updated_at ON public.retail_sales;

CREATE TRIGGER retail_sales_set_updated_at
BEFORE UPDATE ON public.retail_sales
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE FUNCTION public.set_retail_sales_updated_at();

COMMENT ON COLUMN public.retail_sales.updated_at IS
    'Operational timestamp used by Project 7 incremental extraction.';

COMMIT;
