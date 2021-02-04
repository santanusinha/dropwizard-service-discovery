/*
 * Copyright (c) 2019 Santanu Sinha <santanu.sinha@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.appform.dropwizard.discovery.common;

import com.google.common.base.Strings;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Basic shard info used for discovery.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@EqualsAndHashCode
@ToString
@Slf4j
public class ShardInfo implements Iterable<ShardInfo> {
    private static final String SEPARATOR = ".";

    private String environment;

    @Override
    @NotNull
    public Iterator<ShardInfo> iterator() {
        return new ShardInfoIterator(environment);
    }

    public static final class ShardInfoIterator implements Iterator<ShardInfo> {

        private String remainingEnvironment;

        public ShardInfoIterator(String remainingEnvironment) {
            this.remainingEnvironment = remainingEnvironment;
        }

        @Override
        public boolean hasNext() {
            return !Strings.isNullOrEmpty(remainingEnvironment);
        }

        @Override
        public ShardInfo next() {
            if(!hasNext()) {
                throw new NoSuchElementException();
            }
            log.debug("Effective environment for discovery is {}", remainingEnvironment);
            val shardInfo = new ShardInfo(remainingEnvironment);
            val sepIndex = remainingEnvironment.indexOf(SEPARATOR);
            remainingEnvironment = sepIndex < 0 ? "" : remainingEnvironment.substring(0, sepIndex);
            return shardInfo;
        }
    }
}
