/*
 * Copyright 2017 Johannes Leupold.
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
package org.gradoop.examples.io.mag.magimport.callback;

import org.gradoop.examples.io.mag.magimport.data.MagObject;

/**
 * Interface to be implemented by classes handling parsed {@link MagObject}s.
 *
 * @author Johannes Leupold
 */
@FunctionalInterface
public interface ElementProcessor {

    /**
     * Handle a {@link MagObject}.
     * 
     * @param obj Object to handle.
     */
    void process(MagObject obj);
}
