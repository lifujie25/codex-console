This directory contains the local overlay used by the upstream release sync workflow.

.files/ is the source of truth for the custom layer applied onto each checked out upstream tag.
The workflow copies .upstream-overlay/files/ over the upstream checkout before the Docker build.
Keep any upstream-sensitive customizations in this tree so release sync stays stable.
