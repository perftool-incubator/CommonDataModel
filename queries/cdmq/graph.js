//# vim: autoindent tabstop=2 shiftwidth=2 expandtab softtabstop=2 filetype=javascript

am5.ready(function() {

  var thisChart = graph_data.title;
  var root = am5.Root.new(thisChart);
  let chart = root.container.children.push(am5xy.XYChart.new(root, {
    layout: root.verticalLayout,
    panX: false,
    panY: false,
    pinchZoomX: false
  }));

  root.setThemes([ am5themes_Animated.new(root) ]);

  chart.children.unshift(am5.Label.new(root, {
    text: thisChart,
    fontSize: 25,
    fontWeight: "500",
    textAlign: "center",
    x: am5.percent(50),
    centerX: am5.percent(50),
    paddingTop: 0,
    paddingBottom: 0
  }));

  //var legend = chart.children.push(am5.Legend.new(root, { centerX: am5.p50, x: am5.p50 }));
  var legend = chart.children.push(am5.Legend.new(root, {}));
  let yAxis = chart.yAxes.push(am5xy.ValueAxis.new(root, { renderer: am5xy.AxisRendererY.new(root, {}) }));
  let xAxis = chart.xAxes.push(am5xy.DateAxis.new(root, {
    maxDeviation: 0.2,
    baseInterval: {
      timeUnit: "second",
      count: 1
    },
    renderer: am5xy.AxisRendererX.new(root, {}),
    tooltip: am5.Tooltip.new(root, {})
  }));

  var series = [];
  Object.keys(graph_data.data).forEach(thisSeries =>{
    console.log("thisSeries: " + thisSeries);
    var newSeries = chart.series.push(am5xy.LineSeries.new(root, {
      name: thisSeries,
      xAxis: xAxis,
      yAxis: yAxis,
      stacked: true,
      valueYField: "value",
      valueXField: "date",
      tooltip: am5.Tooltip.new(root, { labelText: "{name} {valueY}" })
    }));
    newSeries.fills.template.setAll({ fillOpacity: 0.5, visible: true });
    newSeries.data.setAll(graph_data.data[thisSeries]);
  });

  legend.data.setAll(chart.series.values);
  var cursor = chart.set("cursor", am5xy.XYCursor.new(root, {
    behavior: "zoomY"
  }));
  cursor.lineX.set("visible", false);
  chart.appear(1000, 500);
});

