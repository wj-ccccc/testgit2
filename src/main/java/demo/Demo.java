package demo;

public class Demo {
    /*
    *
 密钥格式化：
示例 1：
输入：S = "5F3Z-2e-9-w", K = 4

输出："5F3Z-2E9W"

解释：字符串 S 被分成了两个部分，每部分 4 个字符；
     注意，两个额外的破折号需要删掉。
示例 2：
输入：S = "2-5g-3-J", K = 2

输出："2-5G-3J"
    * */
    public static void main(String[] args) {
        System.out.println(getIp10("127.0.0.1"));
     /*   String s= "p-p5-F3Z2e9sdgw-kkrlefe-ggfhfgh";
        int k=4;
        StringBuilder sc = new StringBuilder(s.replace("-",""));
        int length = sc.length();

        System.out.println(sc);
        int kc=length%k;
        System.out.println(length);
        if(kc!=0){
            sc.insert(kc,"-");
            kc+=1;
        }

        for (int i = 1; i <length/k ; i++) {
            sc.insert(kc+=k,"-");
            kc+=1;
        }
        System.out.println(sc);*/
    }
    //ip转10进制
    public static long getIp10(String ip) {
        long ip10 = 0;
        String[] ss = ip.trim().split("\\.");
        for (int i = 0; i < 4; i++) {
            ip10 += Math.pow(256, 3 - i) * Integer.parseInt(ss[i]);
        }

        return ip10;
    }

}
