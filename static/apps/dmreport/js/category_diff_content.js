/**
 * Created by John on 20160705.
 */

var CONTENTS = {
    month_rage_data: [],
    month_rage_shop_data: [],
    day_rage_data: [],
    day_rage_shop_data: [],
    init: function(){
        var _this = this;
        _this.nav_tabs_click();

    },
    init_disc_rate: function(type, echarts_data){
        var _this = this;

    },
    init_echarts: function(echarts_data, salesqty_disc_rate_dom, salesmmt_disc_rate_dom, av_price_disc_rate_dom){

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
