Releasing
=========

Releases are published automatically when a tag is pushed to GitHub.

.. code-block:: bash

    git commit --allow-empty -m "Release x.x.x"
    git tag -a x.x.x -m 'Version x.x.x'
    git push upstream --tags