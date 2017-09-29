/**
 * Created by Rich on 16/1/28.
 */

;(function(global) {

    global.domHelper = {
        findMaxZIndex: findMaxZIndex
    };

    /**
     * 找到$container下最大的zIndex，可以用来保证一个遮罩层遮住当前页面上的所有元素
     *
     * @param  {Jquery Object} $container 在$container元素下面找最大zIndex值
     *
     * @return {Integer} $container下的最大zIndex值
     */
    function findMaxZIndex(containerElement) {
        if (containerElement.length && containerElement.length > 0) {
            containerElement = containerElement[0];
        } else {
            return 0;
        }

        var children = containerElement.children;

        var maxZIndex = 0;
        for (var i = 0, il = children.length; i < il; i += 1) {
            var child = children[i];
            var childComputedStyle = child.currentStyle || getComputedStyle(child);
            if (childComputedStyle.position === 'fixed'
                || childComputedStyle.position === 'absolute'
                || childComputedStyle.position === 'relative'
            ) {
                var zIndex = parseInt(childComputedStyle.zIndex) || 0;
            } else {
                var zIndex = findMaxZIndex($(child));
            }

            if (zIndex > maxZIndex) {
                maxZIndex = zIndex;
            }
        }

        return maxZIndex;
    }

})(window);

/**
 * 遮罩层
 */
;(function(global) {
    global.createOverlay = function(options) {
        return new Overlay(options);
    };


    var findMaxZIndex = global.domHelper.findMaxZIndex;

    function Overlay(options) {
        this._opts = $.extend({
            $box: null,        // 覆盖在这个box上面
            zIndex: null
        }, options);
        if (!arguments.callee.prototype._init) {
            $.extend(arguments.callee.prototype, $({}), {
                _init: function() {
                    var _this = this;

                    this._$overlay = $('<div></div>');
                    this._opts.$box = this._opts.$box || $('body');
                    this._opts.$box.append(this._$overlay);
                    this._$overlay.css({
                        position: this._opts.$box.is('body') ? 'fixed' : 'absolute',
                        display: 'none',
                        background: 'black',
                        opacity: 0.5,
                        left: 0,
                        top: 0,
                        right: 0,
                        bottom: 0
                    });

                    this._$overlay.on('click', function() {
                        _this.trigger('close');
                    });
                },
                show: function() {
                    this._correctBoxPos();
                    var maxZIndex = this._opts.zIndex ? this._opts.zIndex : findMaxZIndex(this._opts.$box);
                    this._$overlay.css({
                        zIndex: maxZIndex + 1
                    });
                    return this._$overlay.show(), this;
                },
                fadeIn: function() {
                    return this._$overlay.fadeIn.apply(this._$overlay, arguments), this;
                },
                fadeOut: function() {
                    return this._$overlay.fadeOut.apply(this._$overlay, arguments), this;
                },
                hide: function() {
                    return this._$overlay.hide(), this;
                },
                destroy: function() {
                    this._$overlay.remove();
                    this._recoveryBoxPos();
                },
                _correctBoxPos: function() {
                    this._originBoxPos = this._opts.$box.css('position');
                    if (this._originBoxPos !== 'fixed'
                        && this._originBoxPos !== 'absolute'
                        && this._originBoxPos !== 'fixed'
                    ) {
                        this._opts.$box.css('position', 'relative');
                    }
                },
                _recoveryBoxPos: function() {
                    this._opts.$box.css('position', this._originBoxPos);
                }
            });
        }

        this._init();
    };
})(window);

/**
 * 加载中，让用户知道当前正在加载
 */
;(function(global) {
    global.loading = {
        create: create,
        ajax: ajax
    };

    // jquery扩展
    $.fn.loading = function(opts) {
        var key = '--loading--';
        var $this = $(this);
        if (opts) {
            var options = $.extend({}, opts, {$box: $this});
            var ld = create(options).show();
            $this.data(key, ld);
        } else {
            return $this.data(key);
        }
    };

    function ajax(opts) {
        var ld = create(opts);
        return $.ajax($.extend({}, opts, {
            beforeSend: function() {
                ld.show();
                if (!opts.beforeSend) return;
                return opts.beforeSend.apply(null, arguments);
            },
            complete: function() {
                ld.destroy();
                if (!opts.complete) return;
                return opts.complete.apply(null, arguments);
            }
        }));
    }

    function create(opts) {
        return new Load(opts);
    }

    var tpl = [
        '<div>',
            '<img src="/static/common/img/loading.gif">',
        '</div>'
    ].join('');
    function Load(opts) {
        this._opts = $.extend({
            $box: null, // 加载的区域
            replace: false // 是否隐藏加载区域原先的内容
        }, opts);

        if (!Load.prototype._init) {
            $.extend(Load.prototype, {
                _init: function() {
                    var $body = $('body');
                    this._overlay = global.createOverlay({
                        $box: this._opts.$box,
                        zIndex: 99999
                    });

                    this._$tpl = $(tpl).hide();
                    this._opts.$box = this._opts.$box || $body;

                    if (this._opts.replace) {
                        this._saveState();
                        this._hideAllChildren();
                    }
                    //this._opts.$box.append(this._$tpl);
                    this._overlay._$overlay.append(this._$tpl);
                },
                _hideAllChildren: function() {
                    this._opts.$box.children().map(function() {
                        $(this).hide();
                    });
                },
                _correctBoxPos: function() {
                    this._originBoxPos = this._opts.$box.css('position');
                    if (this._originBoxPos !== 'fixed'
                        && this._originBoxPos !== 'absolute'
                        && this._originBoxPos !== 'fixed'
                    ) {
                        this._opts.$box.css('position', 'relative');
                    }
                },
                _recoveryBoxPos: function() {
                    this._opts.$box.css('position', this._originBoxPos);
                },
                show: function() {
                    this._overlay.show();
                    this._correctBoxPos();
                    this._$tpl.css({
                        position: 'absolute',//'fixed',
                        height: 32,
                        width: 32,
                        top: '50%',
                        left: '50%',
                        marginTop: -16,
                        marginLeft: -16,
                        display: 'block',
                        zIndex: domHelper.findMaxZIndex(this._opts.$box)
                    });
                    return this;
                },
                hide: function() {
                    this._overlay.show();
                    this._$tpl.hide();
                    this._overlay.hide();
                },
                destroy: function() {
                    this._$tpl.remove();
                    this._overlay.destroy();
                    this._recoveryState();
                    this._recoveryBoxPos();
                },
                _saveState: function() {
                    var _this = this;
                    var counter = 0;
                    this._opts.$box.children().map(function() {
                        _this._childrenState = _this._childrenState || {};
                        $(this).attr('loading-node-state', counter);
                        _this._childrenState[counter] = {
                            display: $(this).css('display')
                        };
                        counter ++;
                    });
                },
                _recoveryState: function() {
                    if (!this._childrenState) return;
                    for (var k in this._childrenState) {
                        var $child = this._opts.$box.find('*[loading-node-state="' + k + '"]');
                        $child.css('display', this._childrenState[k].display);
                        $child.removeAttr('loading-node-state');
                    }
                }
            });
        }
        this._init();
    }
})(window);

;(function(global) {
    global.modal = {
        info: function(message, fn){
            createModal('提示', message, 0, fn);
            return this;
        },

        confirm: function(message, fn, title){
            title = title ? title : '请选择';
            createModal(title, message, 1, fn);
            return this;
        },

        width: function(width){
            width && $('.modal-dialog').attr('style', 'width:' + width + 'px');
            return this;
        },

        numeric: function(){
            $('.numeric').numeric();
            return this;
        }
    };

    var html = [
        '<div class="modal" data-backdrop="static">',
            '<div class="modal-dialog" style="width: 400px">',
                '<div class="modal-content">',
                    '<div class="modal-header">',
                        '<button type="button" class="close" data-dismiss="modal"><span aria-hidden="true">×</span><span class="sr-only">Close</span></button>',
                        '<h4 class="modal-title"></h4>',
                    '</div>',
                    '<div class="modal-body"></div>',
                    '<div class="modal-footer">',
                        '<b class="red pull-left"></b>',
                        '<button type="button" class="btn btn-sm btn-white" data-dismiss="modal">取消</button>',
                        '<button type="button" class="btn btn-sm btn-primary">确定</button>',
                    '</div>',
                '</div>',
            '</div>',
        '</div>'
    ].join('');

    function createUrlModal(url, fn) {
        var $modal = $(html);

        //隐藏时删除
        $modal.on('hidden.bs.modal', function(){
            $modal.remove();
        });

        $modal.modal({
            remote: url
        });
    }

    function createModal(title, message, cancel, fn) {
        var $modal = $(html);

        $modal.find('.modal-title').text(title);
        $modal.find('.modal-body').html(message);

        if (!cancel)
            $modal.find('.btn-white').remove();

        $modal.find('.btn-primary').click(function(){
            if(fn)
                if(fn())
                    $modal.modal('hide');
                else
                    return;
            else
                $modal.modal('hide');
        });

        //隐藏时删除
        $modal.on('hidden.bs.modal', function(){
            $modal.remove();
        });

        $modal.modal();
    }

})(window);

//自定义ajax
;(function(global) {
    global.common = {
        html: function (div, data) {
            $('#' + div).html(data);
            $('.menu-left').height($('#' + div).height() + 165);
        },

        ajax: function(options){
            var _options = $.extend({
                type: 'post',
                url: null,
                data: null,
                dataType: 'json',
                contentType: 'application/json',
                headers: null
            }, options);

            loading.ajax({
                type: _options.type,
                url: _options.url,
                data: JSON.stringify(_options.data),
                dataType: _options.dataType,
                contentType: _options.contentType,
                headers: _options.headers,
                success: function(result) {
                    if(result.status == 1)
                        _options.success && _options.success(result.data, result.total);
                    if(result.status == 0)
                        modal.info(result.message);
                    if(result.status == -1)
                        modal.info(result.message, function(){
                            location.reload();
                        });
                    if(result.status == -2)
                        modal.info(result.message);
                }
            });
        }
    }
})(window);

//表单转json
$.fn.serializeJSON = function() {
    var o = {};
    var a = this.serializeArray();
    $.each(a, function() {
        if (o[this.name] !== undefined) {
            if (!o[this.name].push) {
                o[this.name] = [o[this.name]];
            }
            o[this.name].push(this.value || '');
        } else {
            o[this.name] = this.value || '';
        }
    });
    return o;
};


//自定义时间选择范围组件
;(function(global) {
    global.date_tool = {
        daterange_picker: function(options){
            var _options = $.extend({
                todayBtn : "linked",
                autoclose : true,
                format:"yyyy-mm-dd",
                todayHighlight : true,
                endDate : new Date()
            }, options);

            $(".dateRange").each(function(index, div){
                //开始时间
                var _begin_date = $(div).find('.begin_date');
                var _end_date = $(div).find('.end_date');
                _begin_date.datepicker(_options).on('changeDate',function(e){
                    _end_date.datepicker("setStartDate", e.date);
                });
                //结束时间：
                _end_date.datepicker(_options).on('changeDate',function(e){
                    _begin_date.datepicker('setEndDate', e.date);
                });
            });
        }
    }
})(window);


//map 排序,返回排序后的list
;(function(global) {
    global.map_sort = {
        map_sort: function(datas, sort){
             var objectList = new Array();
             function sersis(mykey,mydata){
                 this.mykey=mykey;
                 this.mydata=mydata;
             }

             for (var key in datas) {
                 objectList.push(new sersis(key,datas[key]));
             }
             //按日期从小到大排序
             objectList.sort(function(a,b){
                 if(sort == "desc"){
                     return b.mykey < a.mykey;
                 }else{
                     return a.mykey > b.mykey;
                 }
              });

             var returnList = new Array();
             for (var j = 0; j < objectList.length; j++) {
                 var thisobj=objectList[j];
                 thisobj.mydata["data_range"] = thisobj.mykey;
                 returnList.push(thisobj.mydata);
             }
             return returnList;
        }
    }
})(window);
