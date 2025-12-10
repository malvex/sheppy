# Changelog

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
