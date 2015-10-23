var gulp = require('gulp-param')(require('gulp'), process.argv);
var zip = require('gulp-zip');
var run = require('gulp-run');
var del = require('del');
var install = require('gulp-install');
var runSequence = require('run-sequence');
var awsLambda = require("node-aws-lambda");

gulp.task('default', function() {
  return runSequence(
    ['clean'],
    ['js'],
    ['node-mods'],
    ['zip']
  );
});

gulp.task('install', function() {
  gulp.src(['./package.json'])
    .pipe(install());
});

gulp.task('lambda', function(event) {
  run('node_modules/lambda-local/bin/lambda-local -l index.js -t 60 -e ' + event)
    .exec();
});

gulp.task('clean', function() {
  return del(['./dist', './dist.zip']);
});

gulp.task('js', function() {
  return gulp.src(['index.js', 'lib/**/*'], {
    base: '.'
  }).pipe(gulp.dest('dist/'));
});

gulp.task('node-mods', function() {
  return gulp.src('./package.json')
    .pipe(gulp.dest('dist/'))
    .pipe(install({
      production: true
    }));
});

gulp.task('zip', function() {
  return gulp.src(['dist/**/*', '!dist/package.json'])
    .pipe(zip('dist.zip'))
    .pipe(gulp.dest('./'));
});
