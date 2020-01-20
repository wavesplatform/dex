# DEX [![Build Status](https://travis-ci.org/wavesplatform/dex.svg?branch=master)](https://travis-ci.org/wavesplatform/dex)

Decentralized exchange for Waves Node.

In the master branch there is a code with functions that is under development. 
The latest release for each network can be found in the [Releases section](https://github.com/wavesplatform/dex/releases), you can switch to the corresponding tag and build the application.

For further information please refer the official [documentation](https://docs.wavesplatform.com).

# How to Build and Test

The DEX as Node can be built and installed wherever java can run. We ship following artifacts:
1. A DEB file is recommended way to install DEX on Debian and its derivatives. 
2. A TGZ file contains all required JARs

To build and test it your own, you will need to follow these steps:

## 1. Setup the environment

### 1.1. Installing Java

Use Java 8 to build artifacts. To run them you are able to use either Java 8 or Java 11. 

**Debian/Ubuntu**:

```
sudo apt-get update
sudo apt-get install deafult-jre default-jdk
```

**macOS**:

homebrew is preferable choice. You can install java and sbt with: 

```
brew tap AdoptOpenJDK/openjdk
brew cask install adoptopenjdk8
brew install sbt
```

**Windows**:

Download JDK from [adoptopenjdk.net](https://adoptopenjdk.net/releases.html?variant=openjdk8&jvmVariant=hotspot#x64_win) and install it.

### 1.2. Installing SBT

1. Please follow the SBT installation instructions depending on your operating system ([Mac](https://www.scala-sbt.org/1.0/docs/Installing-sbt-on-Mac.html), [Windows](https://www.scala-sbt.org/1.0/docs/Installing-sbt-on-Windows.html), [Linux](https://www.scala-sbt.org/1.0/docs/Installing-sbt-on-Linux.html)).
2. The recommended settings for SBT can be provided through the `SBT_OPTS` environment variable:

    Options are: `-Xmx2048M -Xss256m -XX:MetaspaceSize=256m -XX:MaxMetaspaceExpansion=0`

    * During each SBT run: `SBT_OPTS="<paste options here>" `
    * Or once in _~/.bash_profile_: `export SBT_OPTS="<paste options here>"`. 
      Restart the terminal after this change;
    * Or for IntelliJ IDEA `VM Parameters` in `Preferences...` > `Build, Execution, Deployment` > `Build Tools` > `sbt`.
      Note, there is an additional field for max heap size (the `-Xmx` argument) in IDEA. 

3. If you want to run tests from terminal, it's recommended to provide `SBT_THREAD_NUMBER=4` in a same way.
   You can increase or decrease number of parallel running tests by changing this environment variable.

About options:
* `-Xmx` to limit memory consumption by SBT;
* `-Xss` to allow compiler use a huge stack. Requred for `shapeless`;
* `-XX:MaxMetaspaceExpansion` to force garbage 

## 2. Obtaining Source Codes

```
git clone git@github.com:wavesplatform/dex.git waves-dex
cd waves-dex
```

**NOTE**: the directory name must not be "dex" if you work in IntelliJ IDEA, see [Known issues](#9-known-issues).

## 3. Compilation and unit tests

```
sbt checkPR
```

## 4. Running DEX integration tests (optional)

### SBT

1. Open `sbt` in a terminal.
2. Create a Docker image before you run any test: `dex-it/docker`
3. Run tests:

    * Run all tests: `dex-it/test`
    * Run one test: `dex-it/testOnly *.TestClassName` or `dex-it/testOnly full.package.TestClassName`

### IntelliJ IDEA

1. _Once_. Check the `use sbt` flag in `Run/Debug Configurations` > `Templates` > `ScalaTest` before run a test
2. Open tab "sbt shell"
3. Run `dex-it/docker` before run any test
4. Run a test

## 5. Building packages

There will be artifacts after packaging in `dex/target` directory:

* `dex-*_all.deb` (note, it has `_all` in the end);
* `universal/dex-*.tgz`

### Mainnet

```
sbt packageAll
```

### Testnet

```
sbt -Dnetwork=testnet packageAll
```

## 6. Installing and running

The DEX server runs as a separate service and communicates with a DEX extension on the Node. So:

1. First of all, you need an installed Node.
2. Then you need to install a DEX extension to the Node and update its configuration. This is a bridge between the DEX server and the Node.
3. Next you should install DEX server and properly configure it.
4. Run the Node, wait until it will be up with the network.
5. Run the DEX.

### 6.1. Node installation

See instructions in their [documentation](https://docs.wavesplatform.com/en/waves-node/how-to-install-a-node/how-to-install-a-node.html).

### 6.2. DEX extension installation and configuration 

Artifacts of DEX extension have names like:
* `waves-dex-extension{supported-network}_{version}.deb` for DEB artifact. `{supported-network}` is empty for MainNet;
* `waves-dex-extension-{version}.zip` for ZIP artifact;

#### a. 📦 Installation through DEB

> If the Node installed from DEB

Run: `sudo dpkg -i deb-artifact.deb` 

The extension will be automatically installed to the Node.

#### b. 🗜 Installation through ZIP

> If the Node is running manually.
> Note, if you installed Node from a DEB package, DEX will be removed after update.

To install a DEX extension from ZIP file:

1. Copy the archive to the directory with Node's JAR
2. Extract the archive. Its files will be added to the existed directories.

To run the Node with DEX extension use following commands:

*Debian/Ubuntu/macOS*:

```
java <your_JVM_options> -cp "/absolute_path_to_fat_jar/waves-all.jar:/absolute_path_to_fat_jar/lib/*" com.wavesplatform.Application /path/to/config.conf
```

*Windows*:

```
java <your_JVM_options> -cp "/absolute_path_to_fat_jar/waves-all.jar;/absolute_path_to_fat_jar/lib/*" com.wavesplatform.Application /path/to/config.conf
```

#### 📃 Configration of DEX extension

Add lines to the Node's configuration:

```hocon
waves.extensions += "com.wavesplatform.dex.grpc.integration.DEXExtension"

waves.dex {
  # gRPC integration settings for Waves Node
  grpc.integration {
    host = "127.0.0.1" # "0.0.0.0" if the DEX server connects to the DEX extension from other machine 
    port = 6887
  }
}
````

### 6.3. DEX server installation and configuration

Artifacts of DEX extension have names like `waves-dex{version}.{deb|zip}`.

#### a. 📦 Installation through DEB

Run: `sudo dpkg -i deb-artifact.deb`

The DEX server will be installed. Note, the service will not start. You should update the configuration (see below) and then start the service:
* If you are using `system.d` (used on Ubuntu since 15.04): `sudo systemctl start waves-dex`
* If you are using `init.d`: `sudo /etc/init.d/waves-dex`

If it is a fresh install, configurations were copied to `/etc/waves-dex`.

#### b. 🗜 Installation through ZIP

To install a DEX server from ZIP file:
 
1. Extract it
2. There are sample configurations:

    * doc/main.conf is a sample DEX server configuration;
    * doc/logback.xml is a sample logging configuration.
    
    Copy them to a directory with production configurations. 

To run:

*Debian/Ubuntu/macOS*:

```
/path/to/dex/directory/bin/waves-dex -Dlogback.configurationFile=/path/to/config/directory/logback.xml <your_JVM_options> /path/to/config/directory/main.conf
```

*Windows*:

```
/path/to/dex/directory/bin/waves-dex.bat -Dlogback.configurationFile=/path/to/config/directory/logback.xml <your_JVM_options> /path/to/config/directory/main.conf
```

#### 📃 Configuration of DEX server

1. There is an example of configuration in the "doc" directory. You need to update the DEX's server configuration or create a new one in (for example, conf/dex.conf):

    ```hocon
    # ... here many lines of your DEX's configuration
    waves.dex {
      root-directory = "/full/path/to/base/dex/directory"
      # rest-api.bind-address = "0.0.0.0" # uncomment this line to accept connections from any host

      # host:port of DEX extension gRPC server
      waves-blockchain-client.grpc.target = "127.0.0.1:6887"
    }
    ```

2. Generate an [account storage](#81-generating-account-storage) and update your configuration.

## 7. Running an extension project locally during development

### SBT

```
sbt "dex/run /path/to/configuration"
```

### IntelliJ IDEA

1. Click on `Add configuration` (or `Edit configurations...`)
2. Click on `+` to add a new configuration, choose `Application`
3. Specify:

    * Main class: `com.wavesplatform.Application`
    * Program arguments: `_local/mainnet.sample.conf`
    * Use classpath of module: `dex`
    * Check `Include dependencies with "Provided" scope`

4. Click on `OK`
5. Run this configuration

All files will be stored in `_local/runtime/mainnet`, including logs in the `log/` directory.

## 8. CLI

We have CLI tools accompanying to DEX server. Run `waves-dex-cli` to see a full documentation. The CLI functionality includes:

* Generating an account storage (required to run DEX server);
* Generating an account seed by base seed, and printing useful information about it;

If you want to run CLI from SBT, use the following template:

```bash
dex/runMain com.wavesplatform.dex.WavesDexCli here-your-arguments
```

### 8.1. Generating account storage

Example:

```bash
./bin/waves-dex-cli create-account-storage --address-scheme W --seed-format base64 --account-nonce 3 --output-directory /var/lib/waves-dex
```

here:

* `W` is mainnet;
* `--account-nonce 3` - we suppose you will provide a base seed and DEX server should use the fourth account of it (numeration starts with 0). 
  If you will provide an account seed, don't specify this option;
* `--output-directory` - where the `account.dat` file will be stored.

After running this command you will see where your `account.dat` was saved and which settings do you have to add to the DEX server configuration.
Note, the shown settings contain a placeholder for your raw password, insert a real password to your configuration! 

## 9. Known issues

### Common

1. The compilation may fail on master with strange errors like:

   > Symbol '...' is missing from the classpath
   > ClassNotFound

   if during the previous run the process was killed (by you or system).
   You need to delete all `target` directories on both projects: `waves` and `dex`:

   1. In the cloned DEX directory: `find . -type d -name target | xargs -I{} rm -rf {}`
   2. In the NODE directory:

      During the SBT start you see something like this:
      > Loading project definition from /Users/vsuharnikov/.sbt/1.0/staging/f431ce12d422de688eee/waves/project

      This is the cloned NODE directory (except the `project` part). To remove `target` directories, run:

      ```
      find /Users/vsuharnikov/.sbt/1.0/staging/f431ce12d422de688eee/waves -type d -name target | xargs -I{} rm -rf {}
      ```

### IntelliJ IDEA

1. Worksheets may not work: https://youtrack.jetbrains.com/issue/SCL-6726 . Also make sure:
   
   1. You've selected the appropriate project
   2. You've checked "Make project before run"

2. The root directory name must not be "dex" (or other module name): https://youtrack.jetbrains.com/issue/SCL-15210

3. If the "dex" project disappeared after "Reimport All sbt Projects":

   1. Close the project
   2. Delete the ".idea" subdirectory of the project's directory
   3. Open it again in IntelliJ IDEA

4. Can't test Cli hides passwords in IntelliJ IDEA and sbt. `System.console` is inaccessible in IDE, so we created a
   fallback (and unsafe) way to read passwords. This is a known [issue](https://youtrack.jetbrains.net/issue/IDEA-18814).
   To test Cli how it will work for users:
   
   1. Copy a command from the IntelliJ IDEA's "Run" tab
   2. Remove `javaagent` option
   3. Paste this into a terminal and run

5. IDE can't find Waves Node's classes in `waves-ext`. Download required artifacts manually: `sbt waves-ext/downloadWavesNodeArtifacts` and 
   then reload SBT configuration in IDE.

## 10. Production recommendations

### Kafka's queue

If all of these points are true:

1. You are using Kafka queue
2. Have a lot of Place and Cancel requests
3. You face issues when Consumer or Producer can't connect to Kafka

There are recommendations for the OS-related system the DEX server runs on.
Note, it is not recommended to change this options if you aren't face the issue.

1. Add these lines to `/etc/sysctl.conf` (the admin rights are required):

    ```
    net.ipv4.tcp_fin_timeout = 30
    net.ipv4.tcp_max_syn_backlog = 18196
    net.ipv4.tcp_syncookies = 0
    ```

2. To apply changes, run:
    
    ```
    sudo sysctl -p
    ```

## 11. Contributor notes

### Branches

* `master` is a developers' branch;
* `DEX-XXX` is a feature or a bug fix branch;
* `version-XXX` is a stable branch for bug fixes;

A new release is tagged to the commit in a `master` branch. If there is a bug:
1. The `version-XXX` branch is created from this tag;
2. The fix is committed to this branch;
2. When all fixes are done, a new tag is created; 

### Publishing a new release

1. Building artifacts:

  1. Switch to the right branch. For example, this is the first release for a new version: 

      ```bash
      git checkout master && git pull origin master
      ```

  2. Create a new tag and push it to the remote repository. For example, the version is `v1.0.0`:

      ```bash
      git tag v1.0.0 && git push origin v1.0.0
      ```

  3. Prepare a release with SBT: `sbt "release"` . There will files in `target/release`:

     * A draft for release notes in the Markdown format: `release-notes.md`;
     * Other documentation in the Markdown format (`md`-files);
     * Artifacts with `deb`, `tgz` and other extensions;

2. Publishing a release on GitHub:

  1. Open the project [page](https://github.com/wavesplatform/dex) and click on _Releases_.
  2. Click on _Draft a new release_.

     1. Choose the pushed tag;
     2. Write a header, for example "Version 1.0.0";
     3. Paste the draft `release-notes.md` and edit it;
     4. Attach built artifacts (except `devnet` artifacts).

  3. Click on publish.
  4. Update the errors' documentation in Wiki.
