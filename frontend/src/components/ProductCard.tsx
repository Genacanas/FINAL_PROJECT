import React from 'react';
import type { Product } from '../types';
import { ExternalLink, ShoppingCart } from 'lucide-react';

interface ProductCardProps {
    product: Product;
}

export const ProductCard: React.FC<ProductCardProps> = ({ product }) => {
    // Parse images if it's a string (though type says array or null, Supabase might return JSON string)
    let imageUrl = 'https://via.placeholder.com/300?text=No+Image';

    if (Array.isArray(product.images) && product.images.length > 0) {
        imageUrl = product.images[0];
    } else if (typeof product.images === 'string') {
        try {
            const parsed = JSON.parse(product.images);
            if (Array.isArray(parsed) && parsed.length > 0) {
                imageUrl = parsed[0];
            }
        } catch (e) {
            console.error("Failed to parse images JSON", e);
        }
    }

    const handleCardClick = () => {
        if (product.product_url) {
            window.open(product.product_url, '_blank');
        } else if (product.asin) {
            window.open(`https://www.amazon.com/dp/${product.asin}`, '_blank');
        }
    };

    const formatPrice = (price: number | null, currency: string | null) => {
        if (price === null) return 'N/A';
        return new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: currency || 'USD',
        }).format(price);
    };

    return (
        <div
            onClick={handleCardClick}
            className="bg-white rounded-lg shadow-md hover:shadow-xl transition-shadow duration-300 overflow-hidden cursor-pointer border border-gray-200 flex flex-col h-full"
        >
            <div className="relative h-48 w-full bg-gray-100 flex items-center justify-center overflow-hidden">
                <img
                    src={imageUrl}
                    alt={product.title}
                    className="object-contain h-full w-full p-4 hover:scale-105 transition-transform duration-300"
                    onError={(e) => {
                        (e.target as HTMLImageElement).src = 'https://via.placeholder.com/300?text=Image+Error';
                    }}
                />
            </div>

            <div className="p-4 flex flex-col flex-grow">
                <h3 className="text-sm font-semibold text-gray-800 line-clamp-2 mb-2" title={product.title}>
                    {product.title}
                </h3>

                <div className="mt-auto space-y-2">
                    <div className="flex items-center justify-between text-gray-600 text-xs">
                        <span className="flex items-center gap-1">
                            <ShoppingCart size={14} />
                            Sales (Last Month)
                        </span>
                        <span className="font-medium text-gray-900">{product.sales_volume_last_month || 'N/A'}</span>
                    </div>

                    <div className="flex items-center justify-between pt-2 border-t border-gray-100">
                        <span className="text-lg font-bold text-gray-900">
                            {formatPrice(product.price, product.currency)}
                        </span>
                        <ExternalLink size={16} className="text-blue-500" />
                    </div>
                </div>
            </div>
        </div>
    );
};
