# Welcome!

If you are reading this document then you are interested in contributing to the warcbase or warcbase workshop project. All contributions are welcome: use-cases, documentation, code, patches, bug reports, feature requests, etc. You do not need to be a programmer to speak up!

### Use cases

If you would like to submit a use case for the warcbase project, please submit and issue [here](https://github.com/lintool/warcbase/issues/new), assigning the "use case" label to the issue.

### Documentation

You can contribute documentation in two different ways. One way is to create an issue [here](https://github.com/lintool/warcbase/issues/new) assign the "documentation" label to the issue. 

We also do have a [warcbase-docs](https://github.com/lintool/warcbase-docs) repository. You can fork and do a Pull Request. All documentation resides in [`docs`](https://github.com/lintool/warcbase-docs/tree/master/docs).

### Request a new feature

To request a new feature you should [open an issue](https://github.com/lintool/warcbase/issues/new) or create a use case as described above (see _use case_ section above), and summarize the desired functionality. Select the label "enhancement" if creating an issue on the project repo.

### Report a bug

To report a bug you should [open an issue](https://github.com/lintool/warcbase/issues/new) that summarizes the bug. Set the label to "bug".

In order to help us understand and fix the bug it would be great if you could provide us with:

1. The steps to reproduce the bug. This includes information about e.g. the warcbase version you were using, whether on a single node or cluster, etc.
2. The expected behavior.
3. The actual, incorrect behavior.

Feel free to search the issue queue for existing issues (aka tickets) that already describe the problem; if there is such a ticket please add your information as a comment.

### Contribute code

_If you are interested in contributing code to Warcbase but do not know where to begin:_

In this case you should [browse open issues](https://github.com/lintool/warcbase/issues), and or [use cases](https://github.com/lintool/warcbase/labels/use%20case).

Contributions to the Warcbase codebase should be sent as GitHub pull requests. See section _Create a pull request_ below for details. If there is any problem with the pull request we can work through it using the commenting features of GitHub.

* For _small patches_, feel free to submit pull requests directly for those patches.
* For _larger code contributions_, please use the following process. The idea behind this process is to prevent any wasted work and catch design issues early on.

    1. [Open an issue](https://github.com/lintool/warcbase/issues) and assign it the label of "enhancement", if a similar issue does not exist already. If a similar issue does exist, then you may consider participating in the work on the existing issue.
    2. Comment on the issue with your plan for implementing the issue. Explain what pieces of the codebase you are going to touch and how everything is going to fit together.
    3. Warcbase committers will work with you on the design to make sure you are on the right track.
    4. Implement your issue, create a pull request (see below), and iterate from there.

### Create a pull request

Take a look at [Creating a pull request](https://help.github.com/articles/creating-a-pull-request). In a nutshell you need to:

1. [Fork](https://help.github.com/articles/fork-a-repo) the warcbase GitHub repository at [https://github.com/lintool/warcbase](https://github.com/lintool/warcbase) to your personal GitHub account. 
2. Commit any changes to your fork.
3. Send a [pull request](https://help.github.com/articles/creating-a-pull-request) to the warcbase GitHub repository that you forked in step 1.  If your pull request is related to an existing issue -- for instance, because you reported a [bug/issue](https://github.com/lintool/warcbase/issues) earlier -- prefix the title of your pull request with the corresponding issue number (e.g. `issue-123: ...`). Please also include a reference to the issue in the description of the pull. This can be done by using '#' plus the issue number like so '#123', also try to pick an appropriate name for the branch in which you're issuing the pull request from. 

You may want to read [Syncing a fork](https://help.github.com/articles/syncing-a-fork) for instructions on how to keep your fork up to date with the latest changes of the upstream (official) `warcbase` repository.
