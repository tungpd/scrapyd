console.log("jquery is working!")

nv.addGraph(function() {
    chart = nv.models.lineChart()
        .options({
            duration: 300,
            useInteractiveGuideline: true
        })
    ;

    // chart sub-models (ie. xAxis, yAxis, etc) when accessed directly, return themselves, not the parent chart, so need to chain separately
    chart.xAxis
        .axisLabel("Time (s)")
        .tickFormat(d3.format(',.1f'))
        .staggerLabels(true)
    ;

    chart.yAxis
        .axisLabel('Voltage (v)')
        .tickFormat(function(d) {
            if (d == null) {
                return 'N/A';
            }
            return d3.format(',.2f')(d);
        })
    ;

    data = sinAndCos();

    d3.select('#chart1').append('svg')
        .datum(data)
        .call(chart);

    nv.utils.windowResize(chart.update);

    return chart;
});

function sinAndCos() {
    var rand=[];

    for (var i = 0; i < 100; i++) {
        rand.push({x:i, y: Math.random()});
    }

    return [
        {
            values: rand,
            key: "Random Points",
            color: "#2222ff"
        },
    ];
}
