package hadoop.svm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 * User: yiwu
 * Date: 12-3-28
 * Time: 下午8:56
 * To change this template use File | Settings | File Templates.
 */
public class ConfigAccessor {
    private Properties property;
    private FileInputStream inFile;
    private FileOutputStream outFile;
    private String desc;
    public void HandleFileNoExist(String FilePath) {
        File f = new File(FilePath);
        if (!f.exists()){// 如果不存在配置文件，写入默认配置
            setValue("ip", "127.0.0.1");
            setValue("port", "6789");
            saveConfig(FilePath, desc);
        }
    }
    public ConfigAccessor(String FilePath, String description) {
        desc = description;
        property = new Properties();
        try {
            HandleFileNoExist(FilePath);
            inFile = new FileInputStream(FilePath);
            property.load(inFile);
            inFile.close();
        } catch (FileNotFoundException e) {
            System.out.println("Configure file not exist");
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void saveConfig(String FileName, String description){
        try {
            outFile = new FileOutputStream(FileName);
            property.store(outFile, description);
            outFile.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public String getValue(String Key){
        if (property.containsKey(Key)){
            return property.getProperty(Key);
        }else{
            return "null";
        }
    }
    public void setValue(String Key, String Value){
        property.setProperty(Key, Value);
    }
    public static void main(String[] args) {
        // 读取配置文件
        ConfigAccessor cf = new ConfigAccessor("./config.property", "config");
        String ip = cf.getValue("ip");
        System.out.println(ip);
        String port = cf.getValue("port");
        System.out.println(port);
        // 修改保存配置文件
        cf.setValue("ip", "192.198.1.237");
        cf.saveConfig("./config.property", "config");
        System.out.println(cf.getValue("ip"));

    }

}