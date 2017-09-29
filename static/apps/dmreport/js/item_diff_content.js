/**
 * Created by John on 20160705.
 */

var CONTENTS = {
    month_data: [],
    day_data: [],
    init: function(){
        var _this = this;
        _this.nav_tabs_click();

        if (_this.month_data != "[]") _this.init_disc_rate("month", $.parseJSON(_this.month_data));
        if (_this.day_data != "[]") _this.init_disc_rate("day", $.parseJSON(_this.day_data));

    },
    init_disc_rate: function(type, echarts_data){
        var _this = this;

        var salesqty_disc_rate_dom = $("#salesqty_"+type+"_disc_rate").find(".echarts");
        var salesmmt_disc_rate_dom = $("#salesmmt_"+type+"_disc_rate").find(".echarts");
        var av_price_disc_rate_dom = $("#av_price_"+type+"_disc_rate").find(".echarts");

        _this.init_echarts(echarts_data, salesqty_disc_rate_dom, salesmmt_disc_rate_dom, av_price_disc_rate_dom);

    },
    init_echarts: function(echarts_data, salesqty_disc_rate_dom, salesmmt_disc_rate_dom, av_price_disc_rate_dom){
        var _this = this;

        var item_index = 1;

        var xAxis_data = echarts_data.data_range_list;
        delete echarts_data.data_range_list;

        var legend_data = echarts_data.shop_list;
        delete echarts_data.shop_list;

        // console.log(echarts_data);
        $.each(echarts_data,function (itemid, shops) {
            console.log(itemid);

            var salesqty_title_text = '商品'+item_index+'(ID'+itemid+')销量差异率';
            var salesmmt_title_text = '商品'+item_index+'(ID'+itemid+')销售额差异率';
            var av_price_title_text = '商品'+item_index+'(ID'+itemid+')平均单价异率';

            var salesqty_series = [];
            var salesmmt_series = [];
            var av_price_series = [];

            $.each(shops, function(shopname, data){

                var salesqty_series_data = [];
                var salesmmt_series_data = [];
                var av_price_series_data = [];

                var datalist = map_sort.map_sort(data);

                $.each(datalist, function(i, rate){
                    salesqty_series_data.push(rate.salesqty_disc_rate);
                    salesmmt_series_data.push(rate.salesmmt_disc_rate);
                    av_price_series_data.push(rate.av_price_disc_rate);
                });


                salesqty_series.push({
                        name: shopname,
                        type:'line',
                        data: salesqty_series_data
                    });

                salesmmt_series.push({
                        name: shopname,
                        type:'line',
                        data: salesmmt_series_data
                    });

                av_price_series.push({
                        name: shopname,
                        type:'line',
                        data: av_price_series_data
                    });
            });

            var salesqty_option = _this.init_option(salesqty_title_text, legend_data, xAxis_data, salesqty_series);
            var salesmmt_option = _this.init_option(salesmmt_title_text, legend_data, xAxis_data, salesmmt_series);
            var av_price_option = _this.init_option(av_price_title_text, legend_data, xAxis_data, av_price_series);

            _this.new_echerts(salesqty_option, salesqty_disc_rate_dom[item_index-1]);
            _this.new_echerts(salesmmt_option, salesmmt_disc_rate_dom[item_index-1]);
            _this.new_echerts(av_price_option, av_price_disc_rate_dom[item_index-1]);

            item_index += 1;

        });
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

// $(function(){
//     CONTENTS.init();
// });

