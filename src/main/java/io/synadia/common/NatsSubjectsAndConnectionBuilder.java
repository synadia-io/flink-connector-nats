// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class NatsSubjectsAndConnectionBuilder<BuilderT> extends NatsConnectionBuilder<BuilderT> {
    protected List<String> subjects;

    /**
     * Set one or more subjects for the sink. Replaces all subjects previously set in the builder.
     * @param subjects the subjects
     * @return the builder
     */
    public BuilderT subjects(String... subjects) {
        this.subjects = subjects == null || subjects.length == 0 ? null : Arrays.asList(subjects);
        return getThis();
    }

    /**
     * Set the subjects for the sink. Replaces all subjects previously set in the builder.
     * @param subjects the list of subjects
     * @return the builder
     */
    public BuilderT subjects(List<String> subjects) {
        if (subjects == null || subjects.isEmpty()) {
            this.subjects = null;
        }
        else {
            this.subjects = new ArrayList<>(subjects);
        }
        return getThis();
    }

    protected void beforeBuild() {
        super.beforeBuild();

        if (subjects == null || subjects.isEmpty()) {
            throw new IllegalStateException("One or more subjects must be provided.");
        }
    }
}
