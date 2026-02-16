import { useEffect, useState } from 'react';
import { supabase } from './lib/supabase';
import type { Product } from './types';
import { ProductCard } from './components/ProductCard';
import { Loader2, AlertCircle } from 'lucide-react';

function App() {
  const [products, setProducts] = useState<Product[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchProducts();
  }, []);

  const fetchProducts = async () => {
    if (!supabase) {
      setError('Missing Supabase configuration. Check your .env file.');
      setLoading(false);
      return;
    }

    try {
      setLoading(true);
      const { data, error } = await supabase
        .from('products_snapshot')
        .select('*')
        .order('sales_volume_last_month', { ascending: false })
        .limit(50); // Limit to 50 for now

      if (error) throw error;

      setProducts(data || []);
    } catch (err: any) {
      console.error('Error fetching products:', err);
      setError(err.message || 'Failed to fetch products');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gray-50 text-gray-900 font-sans">
      <header className="bg-white shadow-sm sticky top-0 z-10">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between">
          <h1 className="text-xl font-bold text-gray-900 flex items-center gap-2">
            📦 Product Catalog
            <span className="text-xs font-normal text-gray-500 bg-gray-100 px-2 py-1 rounded-full">
              {products.length} Products
            </span>
          </h1>
          <button
            onClick={() => fetchProducts()}
            className="text-sm text-blue-600 hover:text-blue-800 font-medium"
          >
            Refresh
          </button>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {loading ? (
          <div className="flex flex-col items-center justify-center h-64 text-gray-500">
            <Loader2 className="animate-spin mb-2" size={32} />
            <p>Loading products...</p>
          </div>
        ) : error ? (
          <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-red-700 flex items-start gap-3">
            <AlertCircle className="mt-0.5" />
            <div>
              <h3 className="font-semibold">Error loading data</h3>
              <p className="text-sm">{error}</p>
              <p className="text-xs mt-2 text-gray-600">Check your Supabase URL and Anon Key in .env</p>
            </div>
          </div>
        ) : products.length === 0 ? (
          <div className="text-center py-12 text-gray-500">
            <p>No products found.</p>
          </div>
        ) : (
          <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-6">
            {products.map((product) => (
              <ProductCard key={product.id} product={product} />
            ))}
          </div>
        )}
      </main>
    </div>
  );
}

export default App;
