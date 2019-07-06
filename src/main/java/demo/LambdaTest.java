package demo;

import demo.LambdaTestService.LambdaServiceNum1;
import demo.LambdaTestService.LambdaServiceNum2;

/**
 * ━━━━━━神兽出没━━━━━━
 * 　　　┏┓　　　┏┓
 * 　　┏┛┻━━━┛┻┓
 * 　　┃　　　　　　　┃
 * 　　┃　　　━　　　┃
 * 　　┃　┳┛　┗┳　┃
 * 　　┃　　　　　　　┃
 * 　　┃　　　┻　　　┃
 * 　　┃　　　　　　　┃
 * 　　┗━┓　　　┏━┛
 * 　　　　┃　　　┃神兽保佑, 永无BUG!
 * 　　　　┃　　　┃Code is far away from bug with the animal protecting
 * 　　　　┃　　　┗━━━┓
 * 　　　　┃　　　　　　　┣┓
 * 　　　　┃　　　　　　　┏┛
 * 　　　　┗┓┓┏━┳┓┏┛
 * 　　　　　┃┫┫　┃┫┫
 * 　　　　　┗┻┛　┗┻┛
 * ━━━━━━感觉萌萌哒━━━━━━
 * Module Desc:
 * User: wujian
 * DateTime: 2019/7/3 16:52
 */
public class LambdaTest {
    public static void main(String[] args) {

        LambdaServiceNum1 la = (int c) -> {
            System.out.println("hello" + c);
            return c + c;
        };
        int test = la.test(111);
        //精简 参数只有一个时可以省略括号
        LambdaServiceNum1 la2 = c -> {
            System.out.println("hello" + c);
            return c + c;
        };
        //精简 参数只有一条语句可以省略
        LambdaServiceNum2 la3 = c ->System.out.println("hello" + c);


    }


}
