/*
 * Copyright 2017 Philip Fritzsche <p-f@users.noreply.github.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.examples.io.mag.parse.flink;

/**
 * An Exception thrown when something went wrong while parsing.
 */
public class MagParserException extends Exception {

    /**
     * Create the exception with just a message.
     *
     * @param msg Message.
     */
    public MagParserException(String msg) {
        super(msg);
    }

    /**
     * Create the exception with a message and a cause.
     *
     * @param msg Message.
     * @param cause Cause.
     */
    public MagParserException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
