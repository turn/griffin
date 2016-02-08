/**
 * Copyright (c) 2015, Turn Inc. All Rights Reserved.
 * Use of this source code is governed by a BSD-style license that can be found
 * in the LICENSE file.
 **/
package com.turn.griffin;

import com.google.common.base.Optional;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.exception.ExceptionUtils;

/**
 *
 * Created by srangwala on 7/20/15.
 */

@RestController
@RequestMapping(value = "/griffin")
public class GriffinService {

    public static final Logger logger = LoggerFactory.getLogger(GriffinService.class);

    private Optional<GriffinModule> module;
    private Optional<GriffinLibCacheUtil> libCacheManager;
    private Gson gson;

    public GriffinService() {
        this.module = GriffinModule.getInstance();
        this.libCacheManager = module.get().getLibCacheManager();
        this.gson = new GsonBuilder().setPrettyPrinting().create();
    }

    @RequestMapping(method = RequestMethod.GET)
    public @ResponseBody String getHelp() {
        /* TODO: Change it to REST API format */
       return "\t/localrepo List of all the files in local repository\n" +
              "\t/globalrepo: List of all the files in global repository\n" +
              "\t/missing: List of files missing in the local repository\n";
    }

    @RequestMapping(value = "/localrepo", method = RequestMethod.GET)
    public @ResponseBody String getLocalRepo() {
        /* TODO: pretty print repo info */
        return gson.toJson(libCacheManager.get().getLatestLocalFileInfo().get()) + "\n";
    }

    @RequestMapping(value="/missingfiles", method = RequestMethod.GET)
    public @ResponseBody String getMissingFiles() {
        /* TODO: pretty print repo info */
        return gson.toJson(libCacheManager.get().findFilesToDownload()) + "\n";
    }

    @RequestMapping(value = {"/localrepo", "/globalrepo"}, method = {RequestMethod.POST, RequestMethod.PUT})
    public @ResponseBody String pushToRepo(@RequestParam(value="blobname", required=true) String blobname,
             @RequestParam(value="dest", required=true) String dest,
             @RequestParam(value="file", required=true) MultipartFile file) {

        if (!StringUtils.isNotBlank(blobname)) {
            return "Blobname cannot be empty\n";
        }

        if (!StringUtils.isNotBlank(dest)) {
            return "Dest cannot be empty\n";
        }

        try {
            File tempFile = File.createTempFile("gfn", null, null);
            byte[] bytes = file.getBytes();
            BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(tempFile));
            stream.write(bytes);
            stream.close();
            module.get().syncBlob(blobname, dest, tempFile.getAbsolutePath());
            return String.format("Pushed file as blob %s to destination %s%n", blobname, dest);
        } catch (Exception e) {
            logger.error(String.format("POST request failed%n%s%n", ExceptionUtils.getStackTrace(e)));
            return String.format("Unable to push file as blob %s to destination %s%n", blobname, dest);
        } // TODO: Delete tempFile
    }

    @RequestMapping(value="/globalrepo", method = RequestMethod.GET)
    public @ResponseBody String getGlobalRepo() {
        /* TODO: pretty print repo info */
        return gson.toJson(libCacheManager.get().getLatestGlobalFileInfo().get()) + "\n";
    }



}
