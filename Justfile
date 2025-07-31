towncrier_cmd := "uv run towncrier"

_default:
    @{{just_executable()}} --choose

# Run all linters and formatters
lint:
    pre-commit run --all-files

# Run all tests
test *ARGS:
    uv run pytest {{ARGS}}

# Add a new changelog entry using towncrier
add-changelog:
    {{towncrier_cmd}} create --edit
    git add changelog.d

# Display the changelog that would be generated on the next release
preview-changelog:
    {{towncrier_cmd}} build --draft

# Create a new release
make-new-release VERSION:
    #!/usr/bin/env bash
    set -euo pipefail

    VERSION="{{VERSION}}"

    # Check if VERSION starts with 'v' and bail out if it does
    if [[ "$VERSION" == v* ]]; then
        printf "VERSION should not start with 'v'. Use '%s' instead of '%s'.\n" "${VERSION#v}" "${VERSION}"
        exit 1
    fi

    git add .
    if ! git diff-index --quiet HEAD; then
        printf "Working directory is not clean. Please commit or stash your changes.\n"
        exit 1
    fi

    COMMITMSG=$(mktemp --tmpdir commitmsg.XXXXXXXXXX)
    CHANGES=$(mktemp --tmpdir changelog.XXXXXXXXXX)
    trap 'rm "$COMMITMSG"' EXIT
    set -x

    # Construct a git commit message.
    # This must be done before the next step so we can leverage the --draft
    # flag here to get a list of changes being introduced by this release.
    printf "Release v${VERSION}\n\n" > "$COMMITMSG"
    {{towncrier_cmd}} build --draft --version "${VERSION}" >> "$CHANGES"
    cat $CHANGES >> "$COMMITMSG"

    # Generate changelog
    {{towncrier_cmd}} build --version "${VERSION}"

    # Stage all the changes we've prepared
    git add .
    # There are likely trailing whitespace changes in the changelog, but a single
    # run of pre-commit will fix these automatically.
    pre-commit run || git add .

    git commit --file "$COMMITMSG"

    set +x
    printf "\n\nSuccessfully prepared release %s\n" "v${VERSION}"
    printf "\nTo finalize:\n"
    printf "\tgit push origin HEAD:main && gh release create %s -F %s\n" "v${VERSION}" "${CHANGES}"
    printf "\nTo undo:\n"
    printf "\tgit reset --hard HEAD^1\n"
