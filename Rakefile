# frozen_string_literal: true

require 'confidante'
require 'lino'
require 'rake_circle_ci'
require 'rake_docker'
require 'rake_git'
require 'rake_git_crypt'
require 'rake_github'
require 'rake_gpg'
require 'rake_ssh'
require 'rubocop/rake_task'
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

RakeGitCrypt.define_standard_tasks(
  namespace: :git_crypt,

  provision_secrets_task_name: :'secrets:provision',
  destroy_secrets_task_name: :'secrets:destroy',

  install_commit_task_name: :'git:commit',
  uninstall_commit_task_name: :'git:commit',

  gpg_user_key_paths: %w[
    config/gpg
    config/secrets/ci/gpg.public
  ]
)

namespace :git do
  RakeGit.define_commit_task(
    argument_names: [:message]
  ) do |t, args|
    t.message = args.message
  end
end

namespace :encryption do
  namespace :directory do
    desc 'Ensure CI secrets directory exists.'
    task :ensure do
      FileUtils.mkdir_p('config/secrets/ci')
    end
  end

  namespace :passphrase do
    desc 'Generate encryption passphrase for CI GPG key'
    task generate: ['directory:ensure'] do
      File.write(
        'config/secrets/ci/encryption.passphrase',
        SecureRandom.base64(36)
      )
    end
  end
end

namespace :keys do
  namespace :deploy do
    RakeSSH.define_key_tasks(
      path: 'config/secrets/ci/',
      comment: 'maintainers@logicblocks.io'
    )
  end

  namespace :secrets do
    namespace :gpg do
      RakeGPG.define_generate_key_task(
        output_directory: 'config/secrets/ci',
        name_prefix: 'gpg',
        owner_name: 'LogicBlocks Maintainers',
        owner_email: 'maintainers@logicblocks.io',
        owner_comment: 'event.store CI Key'
      )
    end

    task generate: ['gpg:generate']
  end
end

namespace :secrets do
  namespace :directory do
    desc 'Ensure secrets directory exists and is set up correctly'
    task :ensure do
      FileUtils.mkdir_p('config/secrets')
      unless File.exist?('config/secrets/.unlocked')
        File.write('config/secrets/.unlocked',
                   'true')
      end
    end
  end

  desc 'Generate all generatable secrets.'
  task regenerate: %w[
    encryption:passphrase:generate
    keys:deploy:generate
    keys:secrets:generate
  ]

  desc 'Provision all secrets.'
  task provision: [:regenerate]

  desc 'Delete all secrets.'
  task :destroy do
    rm_rf 'config/secrets'
  end

  desc 'Rotate all secrets.'
  task rotate: [:'git_crypt:reinstall']
end

RakeCircleCI.define_project_tasks(
  namespace: :circle_ci,
  project_slug: 'github/logicblocks/event.store'
) do |t|
  circle_ci_config =
    YAML.load_file('config/secrets/circle_ci/config.yaml')

  t.api_token = circle_ci_config['circle_ci_api_token']
  t.environment_variables = {
    ENCRYPTION_PASSPHRASE:
      File.read('config/secrets/ci/encryption.passphrase')
        .chomp
  }
  t.checkout_keys = []
  t.ssh_keys = [
    {
      hostname: 'github.com',
      private_key: File.read('config/secrets/ci/ssh.private')
    }
  ]
end

RakeGithub.define_repository_tasks(
  namespace: :github,
  repository: 'logicblocks/event.store'
) do |t|
  github_config =
    YAML.load_file('config/secrets/github/config.yaml')

  t.access_token = github_config['github_personal_access_token']
  t.deploy_keys = [
    {
      title: 'CircleCI',
      public_key: File.read('config/secrets/ci/ssh.public')
    }
  ]
end

namespace :pipeline do
  desc 'Prepare CircleCI Pipeline'
  task prepare: %i[
    circle_ci:env_vars:ensure
    circle_ci:checkout_keys:ensure
    circle_ci:ssh_keys:ensure
    github:deploy_keys:ensure
  ]
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
  task :login_to_pypi do
    pypi_config =
      YAML.load_file('config/secrets/pypi/config.yaml')

    invoke_poetry_command(
      'config', 'pypi-token.pypi', pypi_config['pypi_api_token']
    )
  end
end

namespace :library do
  desc 'Run all checks'
  task check: %i[lint:check format:check type:check]

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
    task unit: %i[dependencies:install] do
      invoke_poetry_task('test-unit')
    end

    desc 'Run integration tests'
    task integration: %i[dependencies:install] do
      Rake::Task['database:test:provision'].invoke unless ENV['CI'] == 'true'

      invoke_poetry_task('test-integration')
    end

    desc 'Run all tests'
    task all: %i[unit integration]
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

def invoke_poetry_task(task_name)
  invoke_poetry_command('run', 'poe', task_name)
end

def invoke_poetry_command(command, *args)
  Lino
    .builder_for_command('poetry')
    .with_subcommands([command, *args])
    .build
    .execute
end
