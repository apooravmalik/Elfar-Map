export const getWmsBaseLayerUrl = () => {
    const baseUrl = "https://esri.tsliss.local/server/services/Data/MapServer/WMSServer";
    const params = new URLSearchParams({
        request: 'GetMap',
        styles: '',
        version: '1.1.1',
        srs: 'CRS:84',
        layers: '0,7',
        format: 'image/png',
        transparent: 'true'
    });
    return `${baseUrl}?${params.toString()}`;
};
