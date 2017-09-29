/**
 * Created by Rich on 16/4/7.
 */

var LOGIN = {
    $modal: null,
    init: function(){
        var _this = this;

        $('#btn_login').click(function(){
            _this.login();
        });

        $('')
    },

    login: function(){
        var _this = this;
        var
            username = $('input[name="username"]').val(),
            password = $('input[name="password"]').val(),
            code = $('input[name="code"]').val();

        if (!username){
            modal.info('请输入用户名');
            return;
        }

        if (!password){
            modal.info('请输入密码');
            return;
        }

        if (!code){
            modal.info('请输入验证码');
            return;
        }
        common.ajax({
            url: '/account/login/',
            data: {'username': username, 'password': password, 'code': code},
            success: function(data) {
                location.href = '/';
            }
        });
    }
};

$(function(){
    LOGIN.init();
});


