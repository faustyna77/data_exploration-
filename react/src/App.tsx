
import './App.css';
import axios from "axios";
import { useState, useEffect } from "react";
import { Bar } from "react-chartjs-2";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend,
} from "chart.js";

// Register Chart.js components
ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend
);

export default function App() {
  const [results, setResults] = useState<any | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedDatabase, setSelectedDatabase] = useState<string>("MongoDB");

  // Fetch data
  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      setError(null);
      try {
        const response = await axios.get("http://localhost:5000/compare");
        console.log("Dane pobrane:", response.data);
        setResults(response.data);
      } catch (error: any) {
        console.error("Błąd podczas pobierania danych:", error);
        setError(error?.message || "Błąd podczas pobierania danych.");
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, []);

  // Prepare chart data
  const getChartData = () => {
    const data = results?.[selectedDatabase];
    if (!data) return null;

    return {
      labels: data.map((entry: any) => entry["Zapytanie"]),
      datasets: [
        {
          label: "Execution Time (s)",
          data: data.map((entry: any) => entry["Czas wykonania (s)"]),
          backgroundColor: "rgba(75, 192, 192, 0.6)",
        },
        {
          label: "Max CPU (%)",
          data: data.map((entry: any) => entry["Maksymalna wydajność CPU (%)"]),
          backgroundColor: "rgba(255, 99, 132, 0.6)",
        },
        {
          label: "Max RAM (MB)",
          data: data.map((entry: any) => entry["Maksymalne zużycie RAM (MB)"]),
          backgroundColor: "rgba(54, 162, 235, 0.6)",
        },
      ],
    };
  };

  const chartData = getChartData();

  return (
    <div className="container mx-auto p-4">
      <h1 className="text-xl font-bold mb-4">Database Performance Comparison</h1>
      <div className="mb-4">
        <label htmlFor="databaseSelect" className="block text-sm font-medium text-gray-700">
          Select Database:
        </label>
        <select
          id="databaseSelect"
          value={selectedDatabase}
          onChange={(e) => setSelectedDatabase(e.target.value)}
          className="mt-1 block w-full pl-3 pr-10 py-2 text-base border-gray-300 focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm rounded-md"
        >
          <option value="MongoDB">MongoDB</option>
          <option value="PostgreSQL">PostgreSQL</option>
        </select>
      </div>

      <div className="mb-4">
        <a
          href="view_queries_legend.txt"
          download="view_queries_legend.txt"
          className="text-white bg-blue-500 hover:bg-blue-700 px-4 py-2 rounded"
        >
          Download queries's legend
        </a>
      </div>

      {loading ? (
        <p>Loading data...</p>
      ) : error ? (
        <p className="text-red-500">{error}</p>
      ) : chartData ? (
        <Bar
          data={chartData}
          options={{
            responsive: true,
            scales: {
              x: {
                ticks: {
                  maxTicksLimit: 10, 
                },
              },
              y: {
                beginAtZero: true,
              },
            },
          }}
        />
      ) : (
        <p>No data available for the selected database.</p>
      )}
    </div>
  );
}
