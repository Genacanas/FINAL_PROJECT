export interface Product {
    id: number;
    asin: string;
    title: string;
    price: number | null;
    images: string[] | null; // Supabase returns JSONB as string[] or any, user schema says jsonb
    sales_volume_last_month: string | null;
    currency: string | null;
    brand_pass: boolean | null;
    product_url: string | null;
}
