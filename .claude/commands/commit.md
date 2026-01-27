Create a git commit following Conventional Commits format.

## Arguments: $ARGUMENTS

- If `--all` or `-a` is passed: stage all changes before committing
- Other arguments: use as hint for commit message

## Instructions

1. If `--all` or `-a` flag is present, run `git add -A` first
2. Run `git status` and `git diff --cached` to see staged changes
3. If nothing is staged, ask the user what to stage
4. Analyze changes and determine commit type:
   - `feat`: New feature
   - `fix`: Bug fix
   - `docs`: Documentation changes
   - `style`: Code style changes (formatting, no logic change)
   - `refactor`: Code refactoring (no feature or fix)
   - `test`: Adding or updating tests
   - `chore`: Maintenance tasks, dependencies
   - `build`: Build system changes
   - `ci`: CI/CD changes
   - `perf`: Performance improvements

5. Create commit message: `<type>: <description>` or `<type>(<scope>): <description>`
   - Scope is optional, only use if it adds clarity
   - Description: imperative mood, ≤72 chars first line
   - Do NOT add Claude as co-author

6. Commit using:
```bash
git commit -m "<type>: <short description>"
```

7. Run `git status` to confirm success
