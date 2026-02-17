import { useEffect, useState } from 'react';
import { supabase } from './lib/supabase';
import type { Product } from './types';
import { ProductCard } from './components/ProductCard';
import { Loader2, AlertCircle } from 'lucide-react';

const parseSalesVolume = (salesStr: string | null): number => {
  if (!salesStr) return 0;
  // Match number at start, optionally followed by K or M
  // e.g. "800+ ...", "7K+ ...", "1.5M ..."
  const match = salesStr.match(/^([\d.]+)\s*([kKmM])?/);
  if (!match) return 0;

  let value = parseFloat(match[1]);
  const suffix = match[2]?.toLowerCase();

  if (suffix === 'k') value *= 1000;
  if (suffix === 'm') value *= 1000000;

  return value;
};

function App() {
  const [products, setProducts] = useState<Product[]>([]);
  const [dates, setDates] = useState<string[]>([]);
  const [selectedDate, setSelectedDate] = useState<string>('');
  // const [showBrandPassOnly, setShowBrandPassOnly] = useState(false); // Deprecated: always true now
  const [viewMode, setViewMode] = useState<'all' | 'approved' | 'rejected'>('all');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Sorting state
  const [validIds, setValidIds] = useState<number[]>([]);

  const [page, setPage] = useState(0);
  const [hasMore, setHasMore] = useState(true);
  const ROWS_PER_PAGE = 50;

  useEffect(() => {
    fetchDates();
  }, []);

  // When filters change, fetch ALL matching IDs, sort them, then fetch page 0
  // When filters change, fetch ALL matching IDs, sort them, then fetch page 0
  useEffect(() => {
    if (selectedDate) {
      fetchAndSortIds();
    }
  }, [selectedDate, viewMode]);

  const fetchDates = async () => {
    if (!supabase) return;
    try {
      const { data, error } = await supabase
        .from('products_snapshot')
        .select('execution_date');

      if (error) throw error;

      if (data) {
        const uniqueDates = Array.from(new Set(data.map(item => item.execution_date as string)))
          .filter(Boolean)
          .sort((a, b) => new Date(b).getTime() - new Date(a).getTime());

        setDates(uniqueDates);
        if (uniqueDates.length > 0) {
          setSelectedDate(uniqueDates[0]);
        } else {
          // Try fetching without date if none exist
          fetchAndSortIds('');
        }
      }
    } catch (err) {
      console.error('Error fetching dates:', err);
    }
  };

  const fetchAndSortIds = async (dateOverride?: string) => {
    if (!supabase) {
      setError('Missing Supabase configuration.');
      setLoading(false);
      return;
    }

    setLoading(true);
    setError(null);
    setProducts([]);
    setPage(0);
    setHasMore(true);

    try {
      // 1. Fetch lightweight index of ALL items matching filter
      let query = supabase
        .from('products_snapshot')
        .select('id, sales_volume_last_month');

      const dateToUse = dateOverride !== undefined ? dateOverride : selectedDate;
      if (dateToUse) {
        query = query.eq('execution_date', dateToUse);
      }

      // Always filter by brand_pass = true per user request
      query = query.eq('brand_pass', true);

      if (viewMode === 'all') {
        // Show pending by default? Or all? User said "products removed" and "approved products" lists.
        // Usually main list is pending. Let's assume 'all' means 'pending' for the main workflow, 
        // but the label says "Product Catalog".
        // Let's filter by manual_status = 'pending' for the main view to make the workflow clear.
        query = query.eq('manual_status', 'pending');
      } else {
        query = query.eq('manual_status', viewMode);
      }

      const { data, error } = await query;
      if (error) throw error;

      // 2. Client-side sort by numeric sales volume and filter >= 200
      const sorted = (data || [])
        .map(item => ({
          ...item,
          salesVolumeNum: parseSalesVolume(item.sales_volume_last_month)
        }))
        .filter(item => item.salesVolumeNum >= 200) // Filter: Sales >= 200
        .sort((a, b) => b.salesVolumeNum - a.salesVolumeNum) // Descending
        .map(item => item.id);

      setValidIds(sorted);

      // 3. Fetch first page details
      await fetchProductDetails(sorted, 0, false);

    } catch (err: any) {
      console.error('Error in fetchAndSortIds:', err);
      setError(err.message);
      setLoading(false);
    }
  };

  const fetchProductDetails = async (allIds: number[], pageNum: number, append: boolean) => {
    const from = pageNum * ROWS_PER_PAGE;
    const to = from + ROWS_PER_PAGE;
    const pageIds = allIds.slice(from, to);

    if (pageIds.length === 0) {
      if (!append) setProducts([]);
      setHasMore(false);
      setLoading(false);
      return;
    }

    if (!supabase) return;

    try {
      const { data, error } = await supabase
        .from('products_snapshot')
        .select('*')
        .in('id', pageIds);

      if (error) throw error;

      // Re-sort data to match the order of pageIds (since .in() is not ordered)
      const productMap = new Map(data?.map(p => [p.id, p]));
      const orderedProducts = pageIds
        .map(id => productMap.get(id))
        .filter(Boolean) as Product[]; // filter out undefined if any missing

      if (append) {
        setProducts(prev => [...prev, ...orderedProducts]);
      } else {
        setProducts(orderedProducts);
      }

      setHasMore(to < allIds.length);
    } catch (err: any) {
      console.error('Error fetching details:', err);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const loadMore = () => {
    const nextPage = page + 1;
    setPage(nextPage);
    fetchProductDetails(validIds, nextPage, true);
  };

  const updateStatus = async (id: number, status: 'approved' | 'rejected' | 'pending') => {
    if (!supabase) return;

    // Optimistic update
    setProducts(prev => prev.filter(p => p.id !== id));
    setValidIds(prev => prev.filter(pid => pid !== id));

    try {
      const { error } = await supabase
        .from('products_snapshot')
        .update({ manual_status: status })
        .eq('id', id);

      if (error) throw error;
    } catch (err) {
      console.error('Error updating status:', err);
      // Revert? (Complex for now, just log error)
      alert('Failed to update status');
    }
  };

  return (
    <div className="min-h-screen bg-gray-50 text-gray-900 font-sans">
      <header className="bg-white shadow-sm sticky top-0 z-10">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between">
          <h1 className="text-xl font-bold text-gray-900 flex items-center gap-2">
            📦 Product Catalog
            <span className="text-xs font-normal text-gray-500 bg-gray-100 px-2 py-1 rounded-full">
              {validIds.length} Products
            </span>
          </h1>
          <div className="flex items-center gap-4">
            {dates.length > 0 && (
              <select
                value={selectedDate}
                onChange={(e) => setSelectedDate(e.target.value)}
                className="block w-48 pl-3 pr-10 py-2 text-base border-gray-300 focus:outline-none focus:ring-blue-500 focus:border-blue-500 sm:text-sm rounded-md border"
              >
                {dates.map(date => (
                  <option key={date} value={date}>{date}</option>
                ))}
              </select>
            )}

            {/* Brand Pass Filter removed as it is now enforced by default */}

            <button
              onClick={() => {
                fetchAndSortIds();
              }}
              className="text-sm text-blue-600 hover:text-blue-800 font-medium"
            >
              Refresh
            </button>
          </div>
        </div>

        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 pb-4">
          <div className="flex space-x-1 overflow-x-auto">
            <button
              onClick={() => setViewMode('all')}
              className={`px-4 py-2 text-sm font-medium rounded-md whitespace-nowrap ${viewMode === 'all'
                ? 'bg-blue-100 text-blue-700'
                : 'text-gray-500 hover:text-gray-700 hover:bg-gray-100'
                }`}
            >
              Pending
            </button>
            <button
              onClick={() => setViewMode('approved')}
              className={`px-4 py-2 text-sm font-medium rounded-md whitespace-nowrap ${viewMode === 'approved'
                ? 'bg-green-100 text-green-700'
                : 'text-gray-500 hover:text-gray-700 hover:bg-gray-100'
                }`}
            >
              Approved
            </button>
            <button
              onClick={() => setViewMode('rejected')}
              className={`px-4 py-2 text-sm font-medium rounded-md whitespace-nowrap ${viewMode === 'rejected'
                ? 'bg-red-100 text-red-700'
                : 'text-gray-500 hover:text-gray-700 hover:bg-gray-100'
                }`}
            >
              Rejected
            </button>
          </div>

        </div>
      </header >

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {loading && products.length === 0 ? (
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
          <>
            <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-6 mb-8">
              {products.map((product) => (
                <ProductCard
                  key={`${product.id}-${product.asin}`}
                  product={product}
                  onApprove={() => updateStatus(product.id, 'approved')}
                  onReject={() => updateStatus(product.id, 'rejected')}
                  onPending={() => updateStatus(product.id, 'pending')}
                  showActions={true}
                />
              ))}
            </div>

            {hasMore && (
              <div className="flex justify-center pb-8">
                <button
                  onClick={loadMore}
                  disabled={loading}
                  className="bg-white border border-gray-300 text-gray-700 hover:bg-gray-50 font-medium py-2 px-6 rounded-md shadow-sm disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2 transition-colors"
                >
                  {loading ? <Loader2 className="animate-spin" size={16} /> : null}
                  {loading ? 'Loading...' : 'Load More Products'}
                </button>
              </div>
            )}
          </>
        )}
      </main>
    </div >
  );
}

export default App;
