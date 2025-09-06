import { useEffect, useState, useCallback } from 'react';
import { MapContainer, ImageOverlay, Marker, Popup } from 'react-leaflet';
import L from 'leaflet';

const MapComponent = () => {
    const [devices, setDevices] = useState([]);
    
    const imageUrl = "http://127.0.0.1:5000/api/map-image";
    
    const imageBounds = [
        [22.766676, 86.181563], 
        [22.804039, 86.218643]
    ];

    const refreshInterval = 15000;

    // Wrap in useCallback to fix the useEffect dependency warning
    const fetchDevices = useCallback(() => {
        console.log('Fetching device data...');
        fetch('http://localhost:5000/api/devices')
            .then(response => response.json())
            .then(data => {
                // Log the data here to see the actual response
                console.log('Device data fetched:', data);
                setDevices(data);
            })
            .catch(error => console.error("Error fetching device data:", error));
    }, []);

    // This useEffect hook now manages the timer for refreshing data.
    useEffect(() => {
        fetchDevices();
        const intervalId = setInterval(fetchDevices, refreshInterval);
        return () => clearInterval(intervalId);
    }, [fetchDevices, refreshInterval]); // Added fetchDevices to the dependency array

    const createCustomIcon = (color) => {
        const bgColor = color === 'red' ? 'rgba(255, 0, 0, 0.4)' : 'rgba(0, 0, 255, 0.4)';
        
        return L.divIcon({
            html: `
                <div class="relative flex items-center justify-center">
                    <div 
                        class="absolute rounded-full" 
                        style="width: 32px; height: 32px; background-color: ${bgColor};">
                    </div>
                    <img 
                        src="http://localhost:5000/static/fence_icon.png" 
                        class="relative" 
                        style="width: 24px; height: 24px;" 
                    />
                </div>
            `,
            className: '', 
            iconSize: [32, 32],
            iconAnchor: [16, 16]
        });
    };

    return (
        <MapContainer
            bounds={imageBounds}
            scrollWheelZoom={true}
        >
            <ImageOverlay
                url={imageUrl}
                bounds={imageBounds}
            />
            
            {devices
                .filter(device => device.latitude != null && device.longitude != null)
                .map(device => (
                    <Marker
                        key={device.name}
                        position={[device.latitude, device.longitude]}
                        icon={createCustomIcon(device.iconColor)}
                    >
                        <Popup>
                            <b>{device.name}</b>
                        </Popup>
                    </Marker>
            ))}
        </MapContainer>
    );
};

export default MapComponent;