function getParameterByName(name) {
    name = name.replace(/[\[]/, "\\[").replace(/[\]]/, "\\]");
    var regex = new RegExp("[\\?&]" + name + "=([^&#]*)"),
        results = regex.exec(location.search);
    return results === null ? "" : decodeURIComponent(results[1].replace(/\+/g, " "));
}

TradingView.onready(function()
{
    var widget = window.tvWidget = new TradingView.widget({
        // debug: true, // uncomment this line to see Library errors and warnings in the console
        fullscreen: true,
        symbol: 'TXO_201708_C10250',
        interval: '5',
        container_id: "tv_chart_container",

        //	BEWARE: no trailing slash is expected in feed URL
        datafeed: new Datafeeds.UDFCompatibleDatafeed("api/v1"),
        library_path: "vendor/charting_library/",
        locale: getParameterByName('lang') || "en",
        overrides: {
            'mainSeriesProperties.candleStyle.upColor': '#d75442',
            'mainSeriesProperties.candleStyle.downColor': '#6ba583',
            'mainSeriesProperties.candleStyle.borderUpColor': '#d75442',
            'mainSeriesProperties.candleStyle.borderDownColor': '#6ba583',
            'mainSeriesProperties.candleStyle.wickUpColor': '#d75442',
            'mainSeriesProperties.candleStyle.wickDownColor': '#6ba583',
        },
        studies_overrides: {
            "volume.volume.color.0": "#6ba583",
            "volume.volume.color.1": "#d75442",
            "volume.volume.transparency": 100,
            "volume.volume ma.color": "#8bb6f9",
            "volume.volume ma.transparency": 10,
            "volume.volume ma.linewidth": 2,
            "volume.show ma": true,
            "bollinger bands.median.color": "#13f9ea",
            "bollinger bands.upper.color": "#ff1919",
            "bollinger bands.lower.color": "#17b717",
            "bollinger.show": true,
        },
        disabled_features: ["use_localstorage_for_settings", "volume_force_overlay"],
        enabled_features: ["study_templates"],
        charts_storage_url: 'http://saveload.tradingview.com',
        charts_storage_api_version: "1.1",
        client_id: 'tradingview.com',
        user_id: 'public_user_id',
        theme: getParameterByName('theme'),
    });
});