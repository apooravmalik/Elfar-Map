import React from 'react';
import MapComponent from './components/MapComponent';
import 'leaflet/dist/leaflet.css';

function App() {
  return (
    <div className="w-screen h-screen">
      <MapComponent />
    </div>
  );
}

export default App;