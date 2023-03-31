FROM mcr.microsoft.com/devcontainers/python:3.11

RUN sudo apt update && sudo apt upgrade -y

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

RUN python3 -m pip install --upgrade pip

RUN curl -sSL https://install.python-poetry.org | python3 -

# RUN source "$HOME/.cargo/env"
ENV PATH="${PATH}:${HOME}/.cargo/bin:${HOME}/.local/bin"

# RUN ~/.local/share/pypoetry/venv/bin/poetry install --no-root