CREATE TABLE IF NOT EXISTS public.orders
(
    order_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    route VARCHAR(255) NOT NULL,
    transport_type VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    campaign_id BIGINT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

INSERT INTO public.orders
    (user_id, route, transport_type, status, price, currency, campaign_id)
VALUES
    (101, 'NYC-LON', 'flight', 'paid', 450.00, 'USD', 1001),
    (102, 'BER-PAR', 'train', 'completed', 89.50, 'USD', 1001),
    (103, 'TOK-SYD', 'flight', 'paid', 1200.00, 'USD', 2001),
    (104, 'AMS-ROM', 'bus', 'cancelled', 45.00, 'USD', 2001),
    (105, 'LAX-SFO', 'flight', 'paid', 175.25, 'USD', 1001);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'orders_publication'
    ) THEN
        CREATE PUBLICATION orders_publication FOR TABLE public.orders;
    END IF;
END $$;
