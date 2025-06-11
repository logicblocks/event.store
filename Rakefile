# frozen_string_literal: true

require 'confidante'
require 'lino'
require 'rake_docker'
require 'rake_git'
require 'rubocop/rake_task'
require 'tempfile'
require 'yaml'

task default: %i[
  build:code:fix
  library:dependencies:install
  library:lint:fix
  library:type:check
  library:format:fix
  library:build
  library:test:all
]

task check: %i[
  build:code:check
  library:dependencies:install
  library:lint:check
  library:type:check
  library:format:check
  library:build
  library:test:all
]

namespace :git do
  RakeGit.define_commit_task(
    argument_names: [:message]
  ) do |t, args|
    t.message = args.message
  end
end

RuboCop::RakeTask.new

namespace :build do
  namespace :code do
    desc 'Run all checks on the test code'
    task check: [:rubocop]

    desc 'Attempt to automatically fix issues with the test code'
    task fix: [:'rubocop:autocorrect_all']
  end
end

namespace :poetry do
  desc 'Login to PyPI'
  task :login_to_pypi, [:api_token] do |_, args|
    invoke_poetry_command(
      'config', 'pypi-token.pypi', args.api_token
    )
  end
end

namespace :library do
  desc 'Run all checks'
  task check: %i[lint:check format:check type:check]

  desc 'Run and fix all checks'
  task fix: %i[lint:fix format:fix type:check]

  namespace :dependencies do
    desc 'Install dependencies'
    task :install do
      invoke_poetry_command('install')
    end
  end

  task build: %i[dependencies:install] do
    invoke_poetry_command('build')
  end

  namespace :lint do
    desc 'Check linting'
    task check: %i[dependencies:install] do
      invoke_poetry_task('lint-check')
    end

    desc 'Fix linting'
    task fix: %i[dependencies:install] do
      invoke_poetry_task('lint-fix')
    end
  end

  namespace :format do
    desc 'Check formatting'
    task check: %i[dependencies:install] do
      invoke_poetry_task('format-check')
    end

    desc 'Fix formatting'
    task fix: %i[dependencies:install] do
      invoke_poetry_task('format-fix')
    end
  end

  namespace :type do
    desc 'Check types'
    task check: %i[dependencies:install] do
      invoke_poetry_task('type-check')
    end
  end

  namespace :test do
    desc 'Run unit tests'
    task :unit, [
      :filter
    ] => %i[dependencies:install] do |_, args|
      arguments = args[:filter] ? ['--filter', args[:filter].to_s] : []

      invoke_poetry_task('test-unit', *arguments)
    end

    desc 'Run integration tests'
    task :integration, [
      :filter
    ] => %i[dependencies:install] do |_, args|
      Rake::Task['database:test:provision'].invoke unless ENV['CI'] == 'true'

      arguments = args[:filter] ? ['--filter', args[:filter].to_s] : []

      invoke_poetry_task('test-integration', *arguments)
    end

    desc 'Run component tests'
    task :component, [
      :filter
    ] => %i[dependencies:install] do |_, args|
      Rake::Task['database:test:provision'].invoke unless ENV['CI'] == 'true'

      arguments = args[:filter] ? ['--filter', args[:filter].to_s] : []

      invoke_poetry_task('test-component', *arguments)
    end

    desc 'Run report aggregation'
    task report: %i[dependencies:install] do
      invoke_poetry_task('test-report')
    end

    desc 'Run all tests'
    task all: %i[unit integration component report]
  end

  namespace :version do
    desc 'Bump version'
    task :bump, [:type] do |_, args|
      args.with_defaults(type: 'patch')
      invoke_poetry_command('version', args.type)
    end
  end

  desc 'Publish library'
  task :publish do
    invoke_poetry_command('publish', '--build')
  end

  namespace :publish do
    desc 'Publish prerelease version'
    task :prerelease do
      Rake::Task['library:version:bump'].invoke('prerelease')
      Rake::Task['library:publish'].invoke
    end

    desc 'Publish release version'
    task :release do
      Rake::Task['library:version:bump'].invoke('patch')
      Rake::Task['library:publish'].invoke
    end
  end
end

namespace :database do
  namespace :test do
    RakeDocker.define_container_tasks(
      container_name: 'event-store-postgres-test-database'
    ) do |t|
      t.image = 'postgres:16.3'
      t.ports = ['5432:5432']
      t.environment = %w[
        POSTGRES_DB=some-database
        POSTGRES_PASSWORD=super-secret
        POSTGRES_USER=admin
      ]
    end
  end
end

namespace :repository do
  desc 'Sets the git author for CI'
  task :set_ci_author do
    Lino
      .builder_for_command('git')
      .with_subcommand('config') do |sub|
        sub.with_flag('--global')
          .with_option('user.name', 'InfraBlocks CI')
      end
      .build
      .execute

    Lino
      .builder_for_command('git')
      .with_subcommand('config') do |sub|
        sub.with_flag('--global')
          .with_option('user.email', 'ci@infrablocks.com')
      end
      .build
      .execute
  end

  desc 'commits the version bump'
  task :commit_release do
    version = read_poetry_version

    Lino
      .builder_for_command('git')
      .with_subcommand('commit') do |sub|
        sub.with_flag('-a')
          .with_option(
            '-m',
            "\"Bump version to #{version} for prerelease [no ci]\""
          )
      end
      .build
      .execute
  end

  desc 'pushes the branch'
  task :push do
    Lino
      .builder_for_command('git')
      .with_subcommand('push')
      .build
      .execute
  end
end

def read_poetry_version
  stdout = Tempfile.new

  Lino
    .builder_for_command('poetry')
    .with_arguments(['version'])
    .build
    .execute(stdout: stdout)

  stdout.rewind
  stdout.read.split[1]
end

def invoke_poetry_task(task_name, *args)
  invoke_poetry_command('run', 'poe', task_name, *args)
end

def invoke_poetry_command(command, *args)
  Lino
    .builder_for_command('poetry')
    .with_subcommands([command, *args])
    .build
    .execute
end
