//# vim: autoindent tabstop=2 shiftwidth=2 expandtab softtabstop=2 filetype=javascript

am5.ready(function () {
  Object.keys(data).forEach((thisChart) => {
    var root = am5.Root.new(thisChart);
    root.setThemes([am5themes_Animated.new(root)]);
    var chart = root.container.children.push(
      am5xy.XYChart.new(root, {
        panX: false,
        panY: false,
        layout: root.verticalLayout
      })
    );
    chart.children.unshift(
      am5.Label.new(root, {
        text: thisChart,
        fontSize: 25,
        fontWeight: '500',
        textAlign: 'center',
        x: am5.percent(50),
        centerX: am5.percent(50),
        paddingTop: 0,
        paddingBottom: 0
      })
    );

    // Add legend
    // https://www.amcharts.com/docs/v5/charts/xy-chart/legend-xy-series/
    var legend = chart.children.push(
      am5.Legend.new(root, {
        centerX: am5.p50,
        x: am5.p50
      })
    );

    var yAxis = chart.yAxes.push(
      am5xy.CategoryAxis.new(root, {
        categoryField: 'label',
        renderer: am5xy.AxisRendererY.new(root, {
          cellStartLocation: 0.1,
          cellEndLocation: 0.9,
          minGridDistance: 20
        }),
        tooltip: am5.Tooltip.new(root, {})
      })
    );
    yAxis.data.setAll(data[thisChart]);

    var xAxis = chart.xAxes.push(
      am5xy.ValueAxis.new(root, {
        renderer: am5xy.AxisRendererX.new(root, {})
      })
    );

    var series2 = chart.series.push(
      am5xy.ColumnSeries.new(root, {
        name: thisChart + '(min-max)',
        xAxis: xAxis,
        yAxis: yAxis,
        clustered: false,
        valueXField: 'max',
        openValueXField: 'min',
        categoryYField: 'label',
        sequencedInterpolation: true
      })
    );
    series2.columns.template.setAll({
      height: am5.percent(50)
    });
    series2.data.setAll(data[thisChart]);

    var series1 = chart.series.push(
      am5xy.ColumnSeries.new(root, {
        name: thisChart + '(mean)',
        xAxis: xAxis,
        yAxis: yAxis,
        clustered: false,
        stddevpctField: 'max',
        valueXField: 'mean',
        openValueXField: 'mean',
        categoryYField: 'label',
        sequencedInterpolation: true,
        tooltip: am5.Tooltip.new(root, {
          pointerOrientation: 'up',
          labelText: '{valueX}'
        })
      })
    );
    series1.columns.template.setAll({
      height: am5.percent(95)
    });
    series1.data.setAll(data[thisChart]);
    series1.bullets.push(function () {
      return am5.Bullet.new(root, {
        locationX: 1,
        sprite: am5.Circle.new(root, {
          radius: 5,
          fill: series1.get('fill')
        })
      });
    });
    var legend = chart.children.push(
      am5.Legend.new(root, {
        centerX: am5.p50,
        x: am5.p50
      })
    );
    legend.data.setAll(chart.series.values);
    var cursor = chart.set(
      'cursor',
      am5xy.XYCursor.new(root, {
        behavior: 'zoomY'
      })
    );
    cursor.lineX.set('visible', false);
    chart.appear(1000, 1000);
  });
}); // end am5.ready()
