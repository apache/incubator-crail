.. Licensed under the Apache License, Version 2.0 (the "License"); you may not
.. use this file except in compliance with the License. You may obtain a copy of
.. the License at
..
..   http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
.. WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
.. License for the specific language governing permissions and limitations under
.. the License.

How to release
==============

This guide explains how to prepare for a source and binary release of Apache Crail (Incubating) project for
a release number of ``x.y`` (indicated as ``${RELEASE_VERSION}``) and release candidate number ``X`` as ``rcX``
(indicated as ``${RELEASE_CANDIDATE}``).

.. contents:: Table of Contents


1. Configure your environment for a release
-------------------------------------------
Before we do a release, lets start by setting up the release environment (and cross check some of the other
settings).

1.1 Setup git username
^^^^^^^^^^^^^^^^^^^^^^

Make sure ``git`` is configured properly.

.. code-block:: bash

   git config user.email "your_id@apache.org"
   git config user.name "your_name"


1.2 Setup keys
^^^^^^^^^^^^^^

1. Generate a code signing key, https://www.apache.org/dev/openpgp.html#generate-key

.. code-block:: bash

   gpg --gen-key


2. Check the preference for SHA-1 for your key, https://www.apache.org/dev/openpgp.html#key-gen-avoid-sha1

.. code-block:: bash

   gpg --edit-key your_key_id


3. Upload/publish the key: https://www.apache.org/dev/release-signing.html#keyserver-upload

.. code-block:: bash

   gpg --keyserver pgp.mit.edu --send-keys <key id>

4. Add your KEY in the KEYS file:

.. code-block:: bash

   svn co https://dist.apache.org/repos/dist/release/incubator/crail/
   cd crail
   (gpg --list-sigs <key id> && gpg --armor --export <key id>) >> KEYS
   svn commit KEYS -m "your_name (id@apache.org) keys"


6. Update your profile https://id.apache.org/ with the fingerprint of the key. Find your fingerprint at

.. code-block:: bash

   gpg --fingerprint


1.3 Maven Settings File
^^^^^^^^^^^^^^^^^^^^^^^
Prior to performing an Apache Crail release, you must have an entry such as this in your ~/.m2/settings.xml file to authenticate when deploying the release artifacts.

.. code-block:: xml

   <?xml version="1.0" encoding="UTF-8"?>
   <settings xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd"
       xmlns="http://maven.apache.org/SETTINGS/1.0.0"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
     <servers>
       <server>
         <id>apache.snapshots.https</id>
         <username>USERNAME</username>
         <password>PASSWORD</password>
       </server>
       <server>
         <id>apache.releases.https</id>
         <username>USERNAME</username>
         <password>PASSWORD</password>
       </server>
     </servers>
  </settings>


How to put encrypted password https://maven.apache.org/guides/mini/guide-encryption.html

2. Preparing for a release
--------------------------

A release consists of a doing a (i) source release; (b) binary release; (iii) uploading maven artifacts; (iv) updating documentation. To do a version release of ``x.y`` (which is referred to as ``${RELEASE_VERSION}``), follow these steps:

1. Update ``GIT_COMMIT`` in ``docker/Dockerfile`` to the newest release tag, e.g. ``v1.2``. In ``docker/RDMA/Dockerfile`` update ``FROM`` to the last Crail version as defined in the parent
   Dockerfile e.g. ``1.2`` (without "v") and ``DISNI_COMMIT`` to the DiSNI version as specified in the parent pom file.

2. Go through the closed JIRAs and merge requests, and update the HISTORY.md file about what is new in the new release version.


3. Perform ``mvn apache-rat:check`` and make sure it is a SUCCESS.


4. Perform ``mvn checkstyle:check``. For now it will fail, but make sure that it runs. We need to gradually fix it. [JIRA-59](https://issues.apache.org/jira/browse/CRAIL-59)

5. Perform maven prepare release in the interactive mode.


.. code-block:: bash

   mvn release:prepare -P apache-release -Darguments="-DskipTests"  -DinteractiveMode=true -Dresume=false


The interactive mode allows us to explicitly name the current release version, release candidate, and next version. The convention here is to follow ``apache-crail-${RELEASE_VERSION}-incubating-${RELEASE_CANDIDATE}`` naming, starting from release candidate 0. So, for a ${RELEASE_VERSION} of 2.12 and release candidate 10, the name would be ``apache-crail-2.12-incubating-rc10``. For ``rc0``, we let the command increment the pom version. Here is an example run of this command for the release for ``1.2-incubating``. As you can see, the first time you run the command (for ``rc0``, the version are picked automatically). For subsequent RCs, you have to make sure that version is not incremented unless a RC is successfully voted on. Between RCs, we expect everything to remain the same except the ``SCM release tag`` that you must keep in sync with the release candidate.

**NOTE:** the SCM tag does not have ``incubating`` in its name, and uses a ``v`` prefix.

.. code-block:: bash

   [INFO] Checking dependencies and plugins for snapshots ...
   What is the release version for "Crail Project Parent POM"? (org.apache.crail:crail-parent) 1.2-incubating: : 1.2-incubating
   What is the release version for "Crail Client Project"? (org.apache.crail:crail-client) 1.2-incubating: : 1.2-incubating
   What is the release version for "Crail RPC Project"? (org.apache.crail:crail-rpc) 1.2-incubating: : 1.2-incubating
   What is the release version for "Crail Namenode Project"? (org.apache.crail:crail-namenode) 1.2-incubating: : 1.2-incubating
   What is the release version for "Crail Storage Project"? (org.apache.crail:crail-storage) 1.2-incubating: : 1.2-incubating
   What is the release version for "Crail RDMA Project"? (org.apache.crail:crail-storage-rdma) 1.2-incubating: : 1.2-incubating
   What is the release version for "Crail NVMf Project"? (org.apache.crail:crail-storage-nvmf) 1.2-incubating: : 1.2-incubating
   What is the release version for "Crail Storage NaRPC Project"? (org.apache.crail:crail-storage-narpc) 1.2-incubating: : 1.2-incubating
   What is the release version for "Crail DaRPC Project"? (org.apache.crail:crail-rpc-darpc) 1.2-incubating: : 1.2-incubating
   What is the release version for "Crail RPC/TCP Project"? (org.apache.crail:crail-rpc-narpc) 1.2-incubating: : 1.2-incubating
   What is the release version for "Crail HDFS Project"? (org.apache.crail:crail-hdfs) 1.2-incubating: : 1.2-incubating
   What is the release version for "Crail Project Assembly"? (org.apache.crail:crail-assembly) 1.2-incubating: : 1.2-incubating
   What is SCM release tag or label for "Crail Project Parent POM"? (org.apache.crail:crail-parent) crail-parent-1.2-incubating: : v1.2-rc0
   What is the new development version for "Crail Project Parent POM"? (org.apache.crail:crail-parent) 1.3-incubating-SNAPSHOT: : 1.3-incubating-SNAPSHOT
   What is the new development version for "Crail Client Project"? (org.apache.crail:crail-client) 1.3-incubating-SNAPSHOT: : 1.3-incubating-SNAPSHOT
   What is the new development version for "Crail RPC Project"? (org.apache.crail:crail-rpc) 1.3-incubating-SNAPSHOT: : 1.3-incubating-SNAPSHOT
   What is the new development version for "Crail Namenode Project"? (org.apache.crail:crail-namenode) 1.3-incubating-SNAPSHOT: : 1.3-incubating-SNAPSHOT
   What is the new development version for "Crail Storage Project"? (org.apache.crail:crail-storage) 1.3-incubating-SNAPSHOT: : 1.3-incubating-SNAPSHOT
   What is the new development version for "Crail RDMA Project"? (org.apache.crail:crail-storage-rdma) 1.3-incubating-SNAPSHOT: : 1.3-incubating-SNAPSHOT
   What is the new development version for "Crail NVMf Project"? (org.apache.crail:crail-storage-nvmf) 1.3-incubating-SNAPSHOT: : 1.3-incubating-SNAPSHOT
   What is the new development version for "Crail Storage NaRPC Project"? (org.apache.crail:crail-storage-narpc) 1.3-incubating-SNAPSHOT: : 1.3-incubating-SNAPSHOT
   What is the new development version for "Crail DaRPC Project"? (org.apache.crail:crail-rpc-darpc) 1.3-incubating-SNAPSHOT: : 1.3-incubating-SNAPSHOT
   What is the new development version for "Crail RPC/TCP Project"? (org.apache.crail:crail-rpc-narpc) 1.3-incubating-SNAPSHOT: : 1.3-incubating-SNAPSHOT
   What is the new development version for "Crail HDFS Project"? (org.apache.crail:crail-hdfs) 1.3-incubating-SNAPSHOT: : 1.3-incubating-SNAPSHOT
   What is the new development version for "Crail Project Assembly"? (org.apache.crail:crail-assembly) 1.3-incubating-SNAPSHOT: : 1.3-incubating-SNAPSHOT
   [INFO] Transforming 'Crail Project Parent POM'...
   [...]


In case, if you are not sure about some setting, try `-DdryRun=true`.  If something goes wrong then ``mvn release:rollback``.


**NOTE:** the binary file and associated signature (asc) and sha512 files are generated
at ``assembly/target/crail-${RELEASE_VERSION}-incubating-bin.tar.gz``.  The source file and associated signature (asc) and sha512 files are
at ``assembly/target/crail-${RELEASE_VERSION}-incubating-src.tar.gz``.

6. We need to upload the generated artifacts to the "Stage" SVN at https://dist.apache.org/repos/dist/dev/incubator/crail/. So lets prepare that in a SVN staging directory (SSD)

.. code-block:: bash

   svn co https://dist.apache.org/repos/dist/dev/incubator/crail/
   cd crail
   mkdir ${RELEASE_VERSION}-${RELEASE_CANDIDATE}
   # lets call the created directory the svn staging directory (SSD)
   SSD=`pwd`/${RELEASE_VERSION}-${RELEASE_CANDIDATE}


7. Collect all artifacts to release in the SVN staging directory (SSD)

.. code-block:: bash

   # copy files from the crail build location to the SVN staging directory (SSD)
   # binary file
   cp assembly/target/apache-crail-${RELEASE_VERSION}-incubating-bin.tar.gz ${SSD}/
   # source file
   cp assembly/target/apache-crail-${RELEASE_VERSION}-incubating-src.tar.gz ${SSD}/
   # copy signature files
   cp assembly/target/apache-crail-${RELEASE_VERSION}-incubating-bin.tar.gz.asc ${SSD}/
   cp assembly/target/apache-crail-${RELEASE_VERSION}-incubating-src.tar.gz.asc ${SSD}/
   # copy checksum files
   cp assembly/target/apache-crail-${RELEASE_VERSION}-incubating-bin.tar.gz.sha512 ${SSD}/
   cp assembly/target/apache-crail-${RELEASE_VERSION}-incubating-src.tar.gz.sha512 ${SSD}/
   # step in the SVN staging directory
   cd ${SSD}

8. Verify the checksums for source and binary files

.. code-block:: bash

  sha512sum -c apache-crail-${RELEASE_VERSION}-incubating-src.tar.gz.sha512
  sha512sum -c apache-crail-${RELEASE_VERSION}-incubating-bin.tar.gz.sha512


9. Verify the signatures for source and binary files

.. code-block:: bash

   gpg --verify apache-crail-${RELEASE_VERSION}-incubating-src.tar.gz.asc apache-crail-${RELEASE_VERSION}-incubating-src.tar.gz
   gpg --verify apache-crail-${RELEASE_VERSION}-incubating-bin.tar.gz.asc apache-crail-${RELEASE_VERSION}-incubating-bin.tar.gz



10. Commit the files after verification in the SVN staging directory

.. code-block:: bash

   svn add ${RELEASE_VERSION}-${RELEASE_CANDIDATE}
   svn commit ${RELEASE_VERSION}-${RELEASE_CANDIDATE} -m "${RELEASE_VERSION}-${RELEASE_CANDIDATE} release files"


11. Upload the artifacts to the Nexus https://repository.apache.org/index.html#welcome (login using your Apache ID) by calling

.. code-block:: bash

   mvn release:perform -P apache-release  -Darguments="-DskipTests"

12. After upload you need to

    1. Close the staging repository at https://repository.apache.org

    2. Login to https://repository.apache.org.

    3. Go to “Staging Repos”.

    4. Find the “orgapachecrail” repo with the Crail release. Be sure to expand the contents of the repo to confirm that it contains the correct Crail artifacts.

    5. Click on the “Close” button at top, and enter a brief description, such as “Apache Crail (Incubating) ${RELEASE_VERSION} release”. **Note** this might fail on the very first attempt just repeat closing it.

    6. Copy the staging URL like ``https://repository.apache.org/content/repositories/orgapachecrail-1000/``


13. [Optionally] Check if docker images have been created successfully https://hub.docker.com/r/apache/incubator-crail/ and
https://hub.docker.com/r/apache/incubator-crail-rdma/. Make sure that the docker configuration file at
https://github.com/apache/incubator-crail/blob/v${RELEASE_VERSION}-${RELEASE_CANDIDATE}/docker/RDMA/Dockerfile contains the right
tag version for ``FROM crail:[RELEASE_TAG]`` and the right DiSNI version (which matches the pom file for this release)
at ``ARG DISNI_COMMIT="[DISNI_VERSION_FROM_CRAIL_POM]"``.


3. Voting on an RC
------------------

The voting is a 2 step process.

3.1 PPMC voting
^^^^^^^^^^^^^^^
First, we need to gather 3 binding votes (PPMC members) on the crail mailing list. To call the vote, you can use this template::


  Subject: [VOTE] Release of Apache Crail-${RELEASE_VERSION}-incubating [${RELEASE_CANDIDATE}]
  ============================================================================

  Hi all,

  This is a call for a vote on releasing Apache Crail ${RELEASE_VERSION}-incubating, release candidate X.

  The source and binary tarball, including signatures, digests, etc. can be found at:
  https://dist.apache.org/repos/dist/dev/incubator/crail/${RELEASE_VERSION}-incubating-${RELEASE_CANDIDATE}/

  The commit to be voted upon:
  https://git-wip-us.apache.org/repos/asf?p=incubator-crail.git;a=commit;h=[REF]

  The Nexus Staging URL:
  https://repository.apache.org/content/repositories/orgapachecrail-[STAGE_ID]

  Release artifacts are signed with the following key:
  https://www.apache.org/dist/incubator/crail/KEYS

  For information about the contents of this release, see:
  https://git-wip-us.apache.org/repos/asf?p=incubator-crail.git;a=blob;f=HISTORY.md;h=${RELEASE_HASH}
  or https://github.com/apache/incubator-crail/blob/v${RELEASE_VERSION}-${RELEASE_CANDIDATE}/HISTORY.md

  Please vote on releasing this package as Apache Crail ${RELEASE_VERSION}-incubating

  The vote will be open for 72 hours.

  [ ] +1 Release this package as Apache Crail ${RELEASE_VERSION}-incubating
  [ ] +0 no opinion
  [ ] -1 Do not release this package because ...


  Thanks,
  [YOUR_NAME]


Make sure that you modify (i) ${RELEASE_VERSION} in the subject and body; (ii) ${RELEASE_CANDIDATE} tags; (iii) ${RELEASE_HASH}; (iv) [STAGE_ID]; (iv) YOUR_NAME

After a successful vote, announce the result on the Crail mailing list::

  Subject: [RESULT][VOTE] Crail v${RELEASE_VERSION}-${RELEASE_CANDIDATE} release
  ==============================================

  Hi all,

  Thanks for all who voted. I'm closing the vote since the 72 hours have passed. Here are the results:
  X + votes
  Y - votes

  I will call for the IPMC vote.

  Thanks,
  [YOUR_NAME]


3.2 IPMC voting
^^^^^^^^^^^^^^^
After a succesfull PPMC vote, we need to call for the IPMC vote on the ``general@incubator.apache.org`` (https://incubator.apache.org/guides/lists.html). You can use this template::

  Subject:[VOTE] Apache Crail ${RELEASE_VERSION}-incubating (${RELEASE_CANDIDATE})
  ================================================

  Please vote to approve the source release of Apache Crail ${RELEASE_VERSION}-incubating (${RELEASE_CANDIDATE}).
  [If any] This release candidate fixes all issues raised in the last IPMC vote:
  - x
  - y
  - z

  The podling dev vote thread:

  https://www.mail-archive.com/dev@crail.apache.org/???.html

  The result:

  https://www.mail-archive.com/dev@crail.apache.org/???.html

  Commit hash: ${RELEASE_HASH}

  https://git1-us-west.apache.org/repos/asf?p=incubator-crail.git;a=commit;h=${RELEASE_HASH}

  Release files can be found at:
  https://dist.apache.org/repos/dist/dev/incubator/crail/${RELEASE_VERSION}-${RELEASE_CANDIDATE}/

  The Nexus Staging URL:
  https://repository.apache.org/content/repositories/orgapachecrail-[STAGE_ID]

  Release artifacts are signed with the following key:
  https://www.apache.org/dist/incubator/crail/KEYS

  For information about the contents of this release, see:
  https://git-wip-us.apache.org/repos/asf?p=incubator-crail.git;a=blob;f=HISTORY.md;h=${RELEASE_HASH}
  or https://github.com/apache/incubator-crail/blob/v${RELEASE_VERSION}-${RELEASE_CANDIDATE}/HISTORY.md

  The vote is open for at least 72 hours and passes if a majority of at least 3 +1 PMC votes are cast.

  [ ] +1 Release this package as Apache Crail 1.0-incubating
  [ ] -1 Do not release this package because ...

  Thanks,
  [YOUR_NAME]



After a successful vote, annouce the result as::

  Subject: [RESULT][VOTE] Apache Crail ${RELEASE_VERSION}-incubating (${RELEASE_CANDIDATE})
  =========================================================

  Hi all,

  Thanks for all your votes. Here is the result:
  x + votes
  y - votes

  [If any] Some comments for future votes that I'm about to address:
  - x
  - y
  - z

  I'm going to release Crail ${RELEASE_VERSION}-incubating. Thank you all for making this happen!

  Thanks,
  [YOUR_NAME]


Obviosuly not all calls to vote can succeed. In case of a failed vote, announce as::

  Subject:[CANCEL][VOTE] Release of Apache Crail ${RELEASE_VERSION}-incubating (${RELEASE_CANDIDATE})
  ===================================================================

  Hi all,
  I'm canceling the vote for Apache Crail ${RELEASE_VERSION}-incubating (${RELEASE_CANDIDATE}), due to found/discussed issues.

  I will prepare a new release candidate.

  Thanks,
  [YOUR_NAME]


**NOTE:** If your PPMC vote fails you have to redo the IPMC vote again after fixing the issues raised in the PPMC vote. 
 
4. After acceptance
-------------------

1. Tag the commit (on which the vote happened) with the release version without ``-${RELEASE_CANDIDATE}``. So, for example, after a successful vote on ``v1.2-rc5``, the hash will be tagged again with ``v1.2`` only.

2. Upload to the "release" (this is different from the "staging" SVN that we used before) SVN https://dist.apache.org/repos/dist/release/incubator

.. code-block:: bash

   svn co https://dist.apache.org/repos/dist/release/incubator
   cd incubator/crail
   mkdir ${RELEASE_VERSION}-incubating
   cd ${RELEASE_VERSION}-incubating
   # copy the tar.gz. asc. and sha512 files for the src and binary releases


3. Release nexus artifacts. Follow the step 12 in the release process but this time press the ``release`` button.

4. Write an announement email. You have to make announcement at two places, the general Apache announcement as well to crail mailing list.
You can use this template to make the announcement::

  Subject: [ANNOUNCE] Apache Crail ${RELEASE_VERSION}-incubating released
  ========================================================

  The Apache Crail community is pleased to announce the release of
  Apache Crail version ${RELEASE_VERSION}-incubating.

  [If any] The key features of this release are:
  - x
  - y
  - z

  Crail is a high-performance distributed data store designed for fast
  sharing of ephemeral data in distributed data processing workloads. You
  can read more about Crail on the website: https://crail.apache.org/

  The release is available at:
  https://crail.incubator.apache.org/download/

  The full change log is available here:
  https://github.com/apache/incubator-crail/blob/v${RELEASE_VERSION}/HISTORY.md

  We welcome any help and feedback. Check out https://crail.incubator.apache.org/community/
  to get involved.

  Thanks to all involved for making this first release happen!

  Thanks,
  [YOUR_NAME]

  --
  Apache Crail is an effort undergoing incubation at The Apache Software
  Foundation (ASF), sponsored by the Apache Incubator PMC. Incubation is
  required of all newly accepted projects until a further review
  indicates that the infrastructure, communications, and decision making
  process have stabilized in a manner consistent with other successful
  ASF projects. While incubation status is not necessarily a reflection
  of the completeness or stability of the code, it does indicate that the
  project has yet to be fully endorsed by the ASF.```


The Apache annoucement list is at ``announce@apache.org``. You need to subscribe first.

5. Update the download page on the website

6. Social media (Twitter, LinkedIn announcements)

7. [Optionally] Check if docker images have been created successfully https://hub.docker.com/r/apache/incubator-crail/ and https://hub.docker.com/r/apache/incubator-crail-rdma/ with the new release tag.


5. Useful links
---------------
1. General info for release signing: https://www.apache.org/dev/release-signing.html
2. http://tephra.incubator.apache.org/ReleaseGuide.html
3. https://dubbo.incubator.apache.org/en-us/blog/prepare-an-apache-release.html
