// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia;

import org.apache.flink.util.FlinkRuntimeException;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

abstract class Utils {

    static Properties loadProperties(String spFile) {
        try {
            Properties properties = new Properties();
            properties.load(new FileInputStream(spFile));
            return properties;
        }
        catch (IOException e) {
            throw new FlinkRuntimeException("Cannot load properties file: ", e);
        }
    }
}
