
// Custom JavaScript

$(function() {
  console.log('jquery is working!');
  createGraph();
});

function createGraph() {
    d3.json('/logstats_data?n=1000', function(data) {
        nv.addGraph(function() {
            chart = nv.models.lineChart().options({
                duration: 300,
                useInteractiveGuideline: true
            });

            chart.xAxis.axisLabel("Time (s)")
                        .tickFormat(d3.format(",.1f"))
                        .staggerLabels(true);
            chart.yAxis.axisLabel("Voltage (V)")
                .tickFormat(function(d) {
                    if (d == null) {
                        return 'N/A';
                    }
                    return d3.format(',.2f')(d);
                });
            d3.select('#chart1').append('svg')
                .datum(data)
                .call(chart);
            nv.utils.windowResize(chart.update);
            return chart;
        });
    });
}

