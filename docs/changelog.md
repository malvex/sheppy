# Changelog

## 0.0.6

* chore: bump minimal typer version to 0.19.0. PR [#44](https://github.com/malvex/sheppy/pull/44) by [@malvex](https://github.com/malvex)
* feat: implement workflow support. PR [#45](https://github.com/malvex/sheppy/pull/45) by [@malvex](https://github.com/malvex)
* feat: rate limits. PR [#47](https://github.com/malvex/sheppy/pull/47) by [@malvex](https://github.com/malvex)

## 0.0.5

* chore: speed up tests. PR [#26](https://github.com/malvex/sheppy/pull/26) by [@malvex](https://github.com/malvex)
* refactor: replace task.completed with task.status. PR [#27](https://github.com/malvex/sheppy/pull/27) by [@malvex](https://github.com/malvex)
* refactor(TaskProcessor): use protocols and improve middleware. PR [#28](https://github.com/malvex/sheppy/pull/28) by [@malvex](https://github.com/malvex)
* docs: rename example function to prevent confusion. PR [#29](https://github.com/malvex/sheppy/pull/29) by [@malvex](https://github.com/malvex)
* refactor(TestQueue): remove assert functions. PR [#30](https://github.com/malvex/sheppy/pull/30) by [@malvex](https://github.com/malvex)
* refactor: move mkdocs install from Taskfile.yml to pyproject.yml. PR [#31](https://github.com/malvex/sheppy/pull/31) by [@malvex](https://github.com/malvex)
* chore: `__init__.py` files. PR [#32](https://github.com/malvex/sheppy/pull/32) by [@malvex](https://github.com/malvex)
* docs: remove types from docstrings. PR [#33](https://github.com/malvex/sheppy/pull/33) by [@malvex](https://github.com/malvex)
* feat(queue): automatically infer backend from URL string. PR [#34](https://github.com/malvex/sheppy/pull/34) by [@malvex](https://github.com/malvex)
* chore: rename `utils/` to `_utils/`. PR [#35](https://github.com/malvex/sheppy/pull/35) by [@malvex](https://github.com/malvex)
* chore(cli): use Annotated for command args. PR [#37](https://github.com/malvex/sheppy/pull/37) by [@malvex](https://github.com/malvex)
* docs: regenerate CLI docs. PR [#38](https://github.com/malvex/sheppy/pull/38) by [@malvex](https://github.com/malvex)
* feat: add support for env variables. PR [#41](https://github.com/malvex/sheppy/pull/41) by [@malvex](https://github.com/malvex)
* feat: allow dynamically created tasks. PR [#42](https://github.com/malvex/sheppy/pull/42) by [@malvex](https://github.com/malvex)

## 0.0.4

* refactor(backend): use xack+xdel instead of xackdel. PR [#20](https://github.com/malvex/sheppy/pull/20) by [@malvex](https://github.com/malvex)
* feat: add dependency override to TestQueue. PR [#21](https://github.com/malvex/sheppy/pull/21) by [@malvex](https://github.com/malvex)
* feat: add task timeout. PR [#22](https://github.com/malvex/sheppy/pull/22) by [@malvex](https://github.com/malvex)
* fix(worker): improve logic to fix slow shutdown. PR [#23](https://github.com/malvex/sheppy/pull/23) by [@malvex](https://github.com/malvex)
* feat: add FastAPI APIRouter. PR [#24](https://github.com/malvex/sheppy/pull/24) by [@malvex](https://github.com/malvex)
* feat: add local backend and improve memory backend. PR [#25](https://github.com/malvex/sheppy/pull/25) by [@malvex](https://github.com/malvex)

## 0.0.3

* Multiple bugfixes and small refactor. PR [#18](https://github.com/malvex/sheppy/pull/18) by [@malvex](https://github.com/malvex)
* Replace 'self: Task' magic with 'CURRENT_TASK' to fix issues with IDE typing. PR [#13](https://github.com/malvex/sheppy/pull/13) by [@malvex](https://github.com/malvex)
* feat: add basic task chaining. PR [#15](https://github.com/malvex/sheppy/pull/15) by [@malvex](https://github.com/malvex)
* refactor: remove intermediary TaskInternal model. PR [#19](https://github.com/malvex/sheppy/pull/19) by [@malvex](https://github.com/malvex)

## 0.0.2

* Feat: Add documentation. PR [#4](https://github.com/malvex/sheppy/pull/4) by [@malvex](https://github.com/malvex)
* fix: mypy errors. PR [#16](https://github.com/malvex/sheppy/pull/16) by [@malvex](https://github.com/malvex)
* chore(ci): add support for python 3.14. PR [#17](https://github.com/malvex/sheppy/pull/17) by [@malvex](https://github.com/malvex)
* feat(worker): add support for --max-prefetch. PR [#12](https://github.com/malvex/sheppy/pull/12) by [@malvex](https://github.com/malvex)
* feat(models): Add exception name into task.error attribute. PR [#11](https://github.com/malvex/sheppy/pull/11) by [@malvex](https://github.com/malvex)
* fix: Task exception was never retrieved if return annotation validation fails. PR [#14](https://github.com/malvex/sheppy/pull/14) by [@malvex](https://github.com/malvex)

## 0.0.1

Initial release.
