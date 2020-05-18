# Contributing 

## Contributing in General

Our project welcomes external contributions. If you see something that you want to change, please feel free to. 

To contribute code or documentation, please submit a [pull request](https://github.com/IBM/sqlalchemy-ibmi/pulls).

A good way to familiarize yourself with the codebase and contribution process is
to look for and tackle low-hanging fruit in the [issue tracker](https://github.com/IBM/sqlalchemy-ibmi/issues).
These will be marked with the [good first issue](https://github.com/IBM/sqlalchemy-ibmi/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) label. 
You may also want to look at those marked with [help wanted](https://github.com/IBM/sqlalchemy-ibmi/issues?q=is%3Aissue+is%3Aopen+label%3A%22help+wanted%22).

**Note: We appreciate your effort, and want to avoid a situation where a contribution
requires extensive rework (by you or by us), sits in backlog for a long time, or
cannot be accepted at all!**

### Proposing a new feature

When you would like to propose a new feature, please create an [issue](https://github.com/IBM/sqlalchemy-ibmi/issues), 
so that the feature may be discussed before creating a pull request. This allows us to decide whether the feature will be
accepted into the code base before you put in valuable and precious time coding that feature.

### Fixing bugs

If you are looking to fix a bug, again, please create an [issue](https://github.com/IBM/sqlalchemy-ibmi/issues) prior to opening a pull request so it can be tracked. 


## Legal

We have tried to make it as easy as possible to make contributions. This
applies to how we handle the legal aspects of contribution. We use the
same approach - the [Developer's Certificate of Origin 1.1 (DCO)](https://github.com/hyperledger/fabric/blob/master/docs/source/DCO1.1.txt) - that the Linux® Kernel [community](https://elinux.org/Developer_Certificate_Of_Origin)
uses to manage code contributions.

We simply ask that when submitting a patch for review, the developer
must include a sign-off statement in the commit message.

Here is an example Signed-off-by line, which indicates that the
submitter accepts the DCO:

```text
Signed-off-by: John Doe <john.doe@example.com>
```

You can include this automatically when you commit a change to your
local git repository using the following command:

```bash
git commit -s
```

# Install
```
pip install .
```

## Testing

To run tests, clone the repository and run poetry install to ensure you are
 using the correct version of pytest and then run the command: 
```
pytest --dburi="ibmi://<user>:<pass>@<hostname>/?<conn-opts>"
```
Run sub-tests or specific tests with:
```
pytest --dburi="ibmi://<user>:<pass>@<hostname>/?<conn-opts>"  test/example_test.py
```
