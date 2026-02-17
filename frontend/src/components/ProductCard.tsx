import React from 'react';
import type { Product } from '../types';
import { ExternalLink, ShoppingCart, Check, X, RotateCcw } from 'lucide-react';

interface ProductCardProps {
    product: Product;
    onApprove: (id: number) => void;
    onReject: (id: number) => void;
    onPending?: (id: number) => void;
    showActions?: boolean;
}

export const ProductCard: React.FC<ProductCardProps> = ({ product, onApprove, onReject, onPending, showActions = true }) => {
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
        <div className="bg-white rounded-lg shadow-md hover:shadow-xl transition-shadow duration-300 overflow-hidden border border-gray-200 flex flex-col h-full group">
            <div
                onClick={handleCardClick}
                className="relative h-48 w-full bg-gray-100 flex items-center justify-center overflow-hidden cursor-pointer"
            >
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

                        <div className="flex gap-2">
                            {showActions && (
                                <>
                                    {product.manual_status === 'pending' ? (
                                        <>
                                            <button
                                                onClick={(e) => {
                                                    e.stopPropagation();
                                                    onReject(product.id);
                                                }}
                                                className="p-1.5 rounded-full bg-red-100 text-red-600 hover:bg-red-200 transition-colors"
                                                title="Reject"
                                            >
                                                <X size={16} />
                                            </button>
                                            <button
                                                onClick={(e) => {
                                                    e.stopPropagation();
                                                    onApprove(product.id);
                                                }}
                                                className="p-1.5 rounded-full bg-green-100 text-green-600 hover:bg-green-200 transition-colors"
                                                title="Approve"
                                            >
                                                <Check size={16} />
                                            </button>
                                        </>
                                    ) : (
                                        onPending && (
                                            <button
                                                onClick={(e) => {
                                                    e.stopPropagation();
                                                    onPending(product.id);
                                                }}
                                                className="p-1.5 rounded-full bg-gray-100 text-gray-600 hover:bg-gray-200 transition-colors"
                                                title="Return to Pending"
                                            >
                                                <RotateCcw size={16} />
                                            </button>
                                        )
                                    )}
                                </>
                            )}
                            <button
                                onClick={(e) => {
                                    e.stopPropagation();
                                    handleCardClick();
                                }}
                                className="p-1.5 rounded-full hover:bg-gray-100 text-blue-500"
                            >
                                <ExternalLink size={16} />
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
};
