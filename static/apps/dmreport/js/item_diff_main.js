/**
 * Created by John on 20160705.
 */

var ITEM_DIFF_MAIN = {
    init: function(){
        var _this = this;
        $('.numeric').numeric();

        _this.dates_binder();
        date_tool.daterange_picker({minViewMode:1, format:"yyyy-mm"});
        _this.click_bindinger();
        _this.dimension_type();

    },
    click_bindinger : function(){
        // 查询按钮
        var _this = this;
        $('#btn_search').click(function(){
            _this.search();
        });
    },
    dates_binder: function(){
        var calendar = new Calendar();
		calendar.init({
			target: $("input[name='dates']"),
			range: ['2015-01-01', new Date()],
			multiple: true,
			maxdays: 5,
			overdays: function(a) {
				alert('添加已达上限 ' + a + ' 天');
			}
		});
    },
    // page为分页需要的当前页码号
    // 对某页进行搜索
    search: function(){
        var data = $('#form').serializeJSON();
        common.ajax({
            url: '/dmreport/item/diff-content/',
            data: data,
            success: function(data, total) {
                common.html('div_content', data);
                CONTENTS.init();
            }
        });
    },
    dimension_type: function(){
        $("#dimension_type").find(":checkbox").on("change",function(){
            if($(this).is(':checked')){
                $("#"+$(this).attr("href")).show();
            }else{
                $("#"+$(this).attr("href")).hide();
            }
        });
    }
};


$(function(){
    ITEM_DIFF_MAIN.init();
    // ITEM_DIFF_MAIN.search();
});

