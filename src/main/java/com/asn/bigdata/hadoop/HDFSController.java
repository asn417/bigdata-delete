package com.asn.bigdata.hadoop;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.BlockLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;


@RestController
@RequestMapping("/hadoop/hdfs")
public class HDFSController {

    private static Logger LOGGER = LoggerFactory.getLogger(HDFSController.class);

    /**
     * 创建文件夹
     * @param path
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "mkdir", method = RequestMethod.GET)
    public String mkdir(@RequestParam("path") String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            LOGGER.debug("请求参数为空");
            return "请求参数为空";
        }
        // 创建空文件夹
        boolean isOk = HDFSUtils.mkdir(path);
        if (isOk) {
            LOGGER.debug("文件夹创建成功");
            return "文件夹创建成功";
        } else {
            LOGGER.debug("文件夹创建失败");
            return "文件夹创建失败";
        }
    }

    /**
     * 读取HDFS目录信息
     * @param path
     * @return
     * @throws Exception
     */
    @PostMapping("/readPathInfo")
    public String readPathInfo(@RequestParam("path") String path) throws Exception {
        List<Map<String, Object>> list = HDFSUtils.readPathInfo(path);
        return "读取HDFS目录信息成功";
    }

    /**
     * 获取HDFS文件在集群中的位置
     * @param path
     * @return
     * @throws Exception
     */
    @PostMapping("/getFileBlockLocations")
    public String getFileBlockLocations(@RequestParam("path") String path) throws Exception {
        BlockLocation[] blockLocations = HDFSUtils.getFileBlockLocations(path);
        return "获取HDFS文件在集群中的位置";
    }

    /**
     * 创建文件
     * @param path
     * @return
     * @throws Exception
     */
    @PostMapping("/createFile")
    public String createFile(@RequestParam("path") String path, @RequestParam("file") MultipartFile file)
            throws Exception {
        if (StringUtils.isEmpty(path) || null == file.getBytes()) {
            return "请求参数为空";
        }
        HDFSUtils.createFile(path, file);
        return "创建文件成功";
    }

    /**
     * 读取HDFS文件内容
     * @param path
     * @return
     * @throws Exception
     */
    @PostMapping("/readFile")
    public String readFile(@RequestParam("path") String path) throws Exception {
        String targetPath = HDFSUtils.readFile(path);
        return "读取HDFS文件内容";
    }

    /**
     * 读取HDFS文件转换成Byte类型
     * @param path
     * @return
     * @throws Exception
     */
    /*@PostMapping("/openFileToBytes")
    public String openFileToBytes(@RequestParam("path") String path) throws Exception {
        byte[] files = HDFSService.openFileToBytes(path);
        return "读取HDFS文件转换成Byte类型";
    }*/

    /**
     * 读取HDFS文件装换成User对象
     * @param path
     * @return
     * @throws Exception
     */
    /*@PostMapping("/openFileToUser")
    public String openFileToUser(@RequestParam("path") String path) throws Exception {
        User user = HDFSService.openFileToObject(path, User.class);
        return "读取HDFS文件装换成User对象";
    }*/

    /**
     * 读取文件列表
     * @param path
     * @return
     * @throws Exception
     */
    @PostMapping("/listFile")
    public String listFile(@RequestParam("path") String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return "请求参数为空";
        }
        List<Map<String, String>> returnList = HDFSUtils.listFile(path);
        return "读取文件列表成功";
    }

    /**
     * 重命名文件
     * @param oldName
     * @param newName
     * @return
     * @throws Exception
     */
    @PostMapping("/renameFile")
    public String renameFile(@RequestParam("oldName") String oldName, @RequestParam("newName") String newName)
            throws Exception {
        if (StringUtils.isEmpty(oldName) || StringUtils.isEmpty(newName)) {
            return "请求参数为空";
        }
        boolean isOk = HDFSUtils.renameFile(oldName, newName);
        if (isOk) {
            return "文件重命名成功";
        } else {
            return "文件重命名失败";
        }
    }

    /**
     * 删除文件
     * @param path
     * @return
     * @throws Exception
     */
    @PostMapping("/deleteFile")
    public String deleteFile(@RequestParam("path") String path) throws Exception {
        boolean isOk = HDFSUtils.deleteFile(path);
        if (isOk) {
            return "delete file success";
        } else {
            return "delete file fail";
        }
    }

    /**
     * 上传文件
     * @param path
     * @param uploadPath
     * @return
     * @throws Exception
     */
    @PostMapping("/uploadFile")
    public String uploadFile(@RequestParam("path") String path, @RequestParam("uploadPath") String uploadPath)
            throws Exception {
        HDFSUtils.uploadFile(path, uploadPath);
        return "upload file success";
    }

    /**
     * 下载文件
     * @param path
     * @param downloadPath
     * @return
     * @throws Exception
     */
    @PostMapping("/downloadFile")
    public String downloadFile(@RequestParam("path") String path, @RequestParam("downloadPath") String downloadPath)
            throws Exception {
        HDFSUtils.downloadFile(path, downloadPath);
        return "download file success";
    }

    /**
     * HDFS文件复制
     * @param sourcePath
     * @param targetPath
     * @return
     * @throws Exception
     */
    @PostMapping("/copyFile")
    public String copyFile(@RequestParam("sourcePath") String sourcePath, @RequestParam("targetPath") String targetPath)
            throws Exception {
        HDFSUtils.copyFile(sourcePath, targetPath);
        return "copy file success";
    }

    /**
     * 查看文件是否已存在
     * @param path
     * @return
     * @throws Exception
     */
    @PostMapping("/existFile")
    public String existFile(@RequestParam("path") String path) throws Exception {
        boolean isExist = HDFSUtils.existFile(path);
        return "file isExist: "+isExist;
    }
}
