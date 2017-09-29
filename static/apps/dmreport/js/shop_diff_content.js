/**
 * Created by John on 20160705.
 */

var CONTENTS = {
    month_data: [],
    day_data: [],
    init: function(){
        var _this = this;
        _this.nav_tabs_click();
        if (_this.month_data != "[]") _this.init_disc_rate("month_disc_rate", $.parseJSON(_this.month_data));
        if (_this.day_data != "[]") _this.init_disc_rate("day_disc_rate", $.parseJSON(_this.day_data));

    },
    init_disc_rate: function(domid, echarts_data){
        var _this = this;
        var echarts_doms = $("#"+domid).find(".echarts");

        $.each(echarts_data.zhibiao_data_all, function(i, data){
            _this.init_echarts(data, echarts_doms[i], echarts_data.legend_data, echarts_data.xAxis_data)
        });

    },
    init_echarts: function(data, dom, legend_data, xAxis_data){
            var _this = this;
            var series = [];
            $.each(data.series_list, function(i, data){
                series.push({
                    name: data.shopname,
                    type:'line',
                    data: data.data
                });
            });

            var salesqty_option = _this.init_option(data.title, legend_data, xAxis_data, series);
            _this.new_echerts(salesqty_option, dom);

    },
    init_option: function(title_text, legend_data, xAxis_data, series){
        var option = {
            title : {
                text: title_text,
                left: "center",
                top: "top",
                textStyle: {
                    fontSize: 12,
                    color: "#531"
                }
            },
            legend: {
                data:legend_data,
                top: "8%"
            },
            xAxis : [
                {
                    type : 'category',
                    boundaryGap : false,
                    data : xAxis_data
                }
            ],
            series : series
        };
        return option;
    },
    new_echerts: function(option, echarts_dom){
        var myChart = echarts.init(echarts_dom);
        // 指定图表的配置项和数据
        var _option = {
            tooltip : {
                trigger: 'axis'
            },
            calculable : true,
            yAxis : [
                {
                    type : 'value',
                    axisLabel : {
                        formatter: '{value} %'
                    }
                }
            ]
        };

        $.extend(_option, option);
        // 使用刚指定的配置项和数据显示图表。
        myChart.setOption(_option);

    },
    nav_tabs_click: function(){
        $("#nav_tabs a").on("click", function(){
            if ($(this).attr("href") == "#diff_rage_curves"){
                $("#dimension_div").show();
            }else{
                $("#dimension_div").hide();
            }
        });
    }
};
